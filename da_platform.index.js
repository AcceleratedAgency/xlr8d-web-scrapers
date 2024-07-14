const { Subject } = require('rxjs');
const amqp = require('amqplib');
const { MongoClient } = require('mongodb');
let {
    CONFIG_KEY,
    RABBITMQ_USER,
    RABBITMQ_PASS,
    RABBITMQ_HOST,
    MESSAGE_BUS_TOPIC,
    ENABLE_DEBUG
} = process.env;
let service_config = {}
function log() {
    if (!ENABLE_DEBUG) return;
    console.log(...arguments);
}
function debounce(f,w) {
    let d=setTimeout(f,w);
    return ()=>{clearTimeout(d);d=setTimeout(f,w);}
}
const subscriptions = [];
function endProcess(msg) { 
    console.warn(msg);
    for (let unsubscribe of subscriptions) try {unsubscribe()}catch(e){console.error(e)}
    console.warn('Exiting in 60sec');
    setTimeout(()=>process.exit(),6e4);
}
let messageBus = null;
let mongo_client = null;
const PROCESS_ID = ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>(c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)); //UUIDv4
async function messageBusInit() {
    let rabbitmq_conn=null;
    let wait = 200;
    while (!!wait--) {//wait for RabbitMQ
        try {
            rabbitmq_conn = await amqp.connect(`amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}`);
            subscriptions.push(_=>rabbitmq_conn.close());
            break;
        } catch(e) { log('waiting for RabbitMQ\n', e)}
        await new Promise(r=>setTimeout(r,1000));
    }
    let queues = new Map();
    if (!rabbitmq_conn) throw new Error('No connection to RabbitMQ found');
    let channel = await rabbitmq_conn.createChannel();
    await channel.assertExchange(MESSAGE_BUS_TOPIC, 'topic', {durable: !1});
    return {
        getQueue: async (queue, prop={durable: !0}) => {
            let c = queues.get(queue);
            if (c) return c;
            let channel = await rabbitmq_conn.createConfirmChannel();
            await channel.assertQueue(queue, prop);
            c = {
                send: (msg,prop)=>{
                    log('Sending data to Queue:', queue, '\n', msg);
                    channel.sendToQueue(queue,Buffer.from((typeof msg != typeof '')? JSON.stringify(msg):msg),prop);
                    return channel.waitForConfirms();
                },
                recv: (fn,prop={noAck:!1}) => {
                    channel.prefetch(1);
                    log('Subscribed to Queue: ', queue);
                    return channel.consume(queue,msg=>{
                        let data=null;
                        try {data=JSON.parse(msg.content.toString())} catch (e) {log('Error parsing JSON from: ', data)}
                        log('Recieved data in Queue:', queue, '\n', data);
                        fn(data,channel,msg);
                    },prop);
                },
                channel
            };
            queues.set(queue,c);
            return c;
        }, 
        publish: (key,msg)=>{
            log('Publishing data to Topic: ', MESSAGE_BUS_TOPIC, '\n', key, '\n', msg);
            return channel.publish(MESSAGE_BUS_TOPIC, key, Buffer.from((typeof msg != typeof '')? JSON.stringify(msg):msg))
        },
        subscribe: async (...keys)=>{
            let {queue} = await channel.assertQueue('',{exclusive: !0});
            for (let key of keys) channel.bindQueue(queue,MESSAGE_BUS_TOPIC,key);
            log('Subscribed to the topic:',MESSAGE_BUS_TOPIC,'\n',keys);
            return (fn,prop={noAck:!0}) => channel.consume(queue,msg=>{
                let data=null;
                try {data=JSON.parse(msg.content.toString())} catch (e) {console.log('Error parsing JSON from: ', data)}
                log('Recieveddata in Topic',MESSAGE_BUS_TOPIC,'\n', msg.fields.routingKey,'\n', data);
                fn({key:msg.fields.routingKey,data},channel,msg);
            },prop);
        }
    }
}
async function configureMessageBus() {
    await messageBus.getQueue(service_config.QUEUE_TASK_TYPE.SCRAPING).then(({recv})=>recv((data,channel,msg)=>{
        log('Recieved task:', data);
        startScraping(data,debounce(endProcess,6e4)).catch(console.error);
        channel.ack(msg);
    })).catch(console.error);
}

async function storeData(slug,db_collection,data) {
    log('Saving data to MongoDB:',{slug,db_collection,data});
    return (await mongo_client.db(slug).collection(db_collection).insertOne(data)).insertedId.toString();
}
let cancelled=!1;
async function startScraping(task,endProcessDelay) {
    let {id,client,slug,config} = task;
    log("Starting scraping task:", {id,client,slug,config});
    const taskSubject = new Subject();
    await messageBus.subscribe(service_config.QUEUE_TASK_TYPE.SCRAPING+".cancel."+id).then(handle=>handle(task=>{
        if (cancelled) return;
        log('Task cancel requested:', id, "\n", task);
        taskSubject.complete();
        taskFinished(id).catch(console.error).finally(()=>endProcess('Job cancelled'));
        cancelled=!0
    })).catch(console.error);
    let sub = taskSubject.subscribe({next: async data=>{
        endProcessDelay();
        log(`Response from scraper: \n`, data);
        let db_doc_id = await storeData(slug,service_config.QUEUE_TASK_TYPE.SCRAPING,data).catch(console.error);
        if (!db_doc_id) return;
        await messageBus.getQueue(service_config.QUEUE_TASK_TYPE.CLASSIFY).then(({send})=>send({
            ...task,
            db_collection: service_config.QUEUE_TASK_TYPE.SCRAPING,
            db_doc_id
        })).catch(console.error);
        endProcessDelay();
    }});
    subscriptions.push(_=>sub.unsubscribe());
    let {scrapers} = require('./browserStack');
    await scrapers(config,taskSubject)
    .then(taskFinished.bind(this,id))
    .catch(e=>{
        console.error('Scraping failed.', e, '\nRemoving task from the queue');
        messageBus.getQueue(service_config.QUEUE_TASK_TYPE.REMOVE_QUEUED).then(({send})=>send({id}));
    });
}
async function taskFinished(id) {
    if (cancelled) return log('task already canceled');
    log('Scraping task Finished:',id);
    return await messageBus.getQueue(service_config.QUEUE_TASK_TYPE.SCRAPING+'.finished').then(({send})=>send({id}));
}
async function prepareVariables(run,die) {
    await messageBus.getQueue(PROCESS_ID,{exclusive:!0}).then(({recv})=>recv((data,ch,msg)=>{
        if (PROCESS_ID != msg.properties.correlationId || typeof {} !== typeof data) throw ch.close();
        Object.assign(service_config,data);
        run();
        ch.close();
    }),{noAck:!0}).catch(die);
    await messageBus.getQueue(CONFIG_QUEUE).then(({send})=>send({CONFIG_KEY}, {replyTo: PROCESS_ID,correlationId: PROCESS_ID})).catch(die);
}
(async ()=>{
    console.log('Starting web-scraper: ', PROCESS_ID);
    messageBus = await messageBusInit();
    await new Promise(prepareVariables);
    mongo_client = new MongoClient(`mongodb://${service_config.MONGODB_USER}:${service_config.MONGODB_PASS}@${service_config.MONGODB_HOST}`);
    let wait = 200;
    while (!!wait--) {//wait for MongoDB
        try {
            await mongo_client.connect();
            subscriptions.push(_=>mongo_client.close());
            break;
        } catch(e) { log('waiting for MongoDB\n', e)}
        await new Promise(r=>setTimeout(r,1000));
    }
    await configureMessageBus();
    console.log("\nReady\n");
})().catch(endProcess);