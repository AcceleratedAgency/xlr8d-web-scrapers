const { Subject } = require('rxjs');
const amqp = require('amqplib');
const { MongoClient } = require('mongodb');
const {
    MONGODB_USER,
    MONGODB_PASS,
    MONGODB_HOST,
    RABBITMQ_USER,
    RABBITMQ_PASS,
    RABBITMQ_HOST,
    MESSAGE_BUS_TOPIC
} = process.env;
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
const mongo_client = new MongoClient(`mongodb://${MONGODB_USER}:${MONGODB_PASS}@${MONGODB_HOST}`);
const PROCESS_ID = ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>(c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)); //UUIDv4
const QUEUE_TASK_TYPE = {
    SCRAPING: 'web_scraping',
    CLASSIFY: 'classification',
    REMOVE_QUEUED: 'da_platform_remove_queued'
}
async function messageBusInit() {
    let rabbitmq_conn=null;
    let wait = 200;
    while (!!wait--) {//wait for RabbitMQ
        try {
            rabbitmq_conn = await amqp.connect(`amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}`);
            subscriptions.push(_=>rabbitmq_conn.close());
            break;
        } catch(e) { console.log('waiting for RabbitMQ\n', e)}
        await new Promise(r=>setTimeout(r,1000));
    }
    let queues = new Map();
    if (!rabbitmq_conn) throw new Error('No connection to RabbitMQ found');
    let channel = await rabbitmq_conn.createChannel();
    await channel.assertExchange(MESSAGE_BUS_TOPIC, 'topic', {durable: !1});
    return {
        getQueue: async queue => {
            let c = queues.get(queue);
            if (c) return c;
            let channel = await rabbitmq_conn.createChannel();
            await channel.assertQueue(queue, {durable: !0});
            c = {
                send: (msg,prop)=>channel.sendToQueue(queue,Buffer.from((typeof msg != typeof '')? JSON.stringify(msg):msg),prop),
                recv: (fn,prop={noAck:!1}) => {
                    channel.prefetch(1);
                    return channel.consume(queue,msg=>{
                        let data=null;
                        try {data=JSON.parse(msg.content.toString())} catch (e) {console.log('Error parsing JSON from: ', data)}
                        fn(data,channel,msg);
                    },prop);
                },
                channel
            };
            queues.set(queue,c);
            return c;
        }, 
        publish: (key,msg)=>channel.publish(MESSAGE_BUS_TOPIC, key, Buffer.from((typeof msg != typeof '')? JSON.stringify(msg):msg)),
        subscribe: async (...keys)=>{
            let {queue} = await channel.assertQueue('',{exclusive: !0});
            for (let key of keys) channel.bindQueue(queue,MESSAGE_BUS_TOPIC,key);
            return (fn,prop={noAck:!0}) => channel.consume(queue,msg=>{
                let data=null;
                try {data=JSON.parse(msg.content.toString())} catch (e) {console.log('Error parsing JSON from: ', data)}
                fn({key:msg.fields.routingKey,data},channel,msg);
            },prop);
        }
    }
}
async function configureMessageBus() {
    messageBus = await messageBusInit();
    //listenters
    await messageBus.getQueue(QUEUE_TASK_TYPE.SCRAPING).then(({recv})=>recv((data,channel,msg)=>{
        startScraping(data,debounce(endProcess,6e4)).catch(console.error);
        channel.ack(msg);
    })).catch(console.error);
}

async function storeData(slug,db_collection,data) {
    return (await mongo_client.db(slug).collection(db_collection).insertOne(data)).insertedId.toString();
}

async function startScraping({id,client,slug,config},endProcessDelay) {
    console.log(config);
    const taskSubject = new Subject();
    await messageBus.subscribe(QUEUE_TASK_TYPE.SCRAPING+".cancel."+id).then(handle=>handle(()=>{
        taskSubject.complete();
        taskFinished(id).catch(console.error).finally(()=>endProcess('Job cancelled'));
    })).catch(console.error);
    let sub = taskSubject.subscribe(async data=>{
        endProcessDelay();
        let db_doc_id = await storeData(slug,QUEUE_TASK_TYPE.SCRAPING,data).catch(console.error);
        if (!db_doc_id) return;
        await messageBus.getQueue(QUEUE_TASK_TYPE.CLASSIFY).then(({send})=>send({
            id,
            client,
            slug,
            db_collection: QUEUE_TASK_TYPE.SCRAPING,
            db_doc_id
        })).catch(console.error);
        endProcessDelay();
    });
    subscriptions.push(_=>sub.unsubscribe());
    let {scrapers} = require('./browserStack');
    await scrapers(config,taskSubject)
    .then(taskFinished.bind(this,id))
    .catch(e=>{
        console.error('Scraping failed.', e, '\nRemoving task from the queue');
        messageBus.getQueue(QUEUE_TASK_TYPE.REMOVE_QUEUED).then(({send})=>send({id}));
    });
    await taskFinished(id).catch(console.error);
}
async function taskFinished(id) {
    return messageBus.getQueue(QUEUE_TASK_TYPE.SCRAPING+'.finished').then(({send})=>send({id}));
}
(async ()=>{
    console.log('Starting web-scraper: ', PROCESS_ID);
    let wait = 200;
    while (!!wait--) {//wait for MongoDB
        try {
            await mongo_client.connect();
            subscriptions.push(mongo_client.close.bind(mongo_client));
            break;
        } catch(e) { console.log('waiting for MongoDB\n', e)}
        await new Promise(r=>setTimeout(r,1000));
    }
    await configureMessageBus();
    console.log("\nReady\n");
})().catch(endProcess);