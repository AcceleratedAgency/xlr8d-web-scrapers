const fs=require("fs");
const path = require('path');
require('dotenv').config();
let webdriver = require('selenium-webdriver');
let delay = +process.env.IDLE_TIMEOUT||3000;
var default_capabilities = JSON.parse(fs.readFileSync(path.join(__dirname,'./default_capabilities.json')));
function log() {
    console.log("[BS scraping] ",...arguments);
}
async function scrapers(config,subject) {
    switch (config.type) {
        case 'dump': return getContentDump(config,subject);
        default: throw new Error('unsuported type');
    }
}
async function getContentDump(config, subject) {
    let {clickPathBefore, clickPathAfter, dump, repeat, name, url} = {
        clickPathBefore: [],
        clickPathAfter: [],
        dump: [],
        name: 'Reviews dumping',
        url: null,
        ...config
    };
    let capabilities={...default_capabilities,...{'bstack:options' : {
		"projectName" : name,
		"buildName" : name,
        "idleTimeout": 10*delay/1000,
        "userName" : process.env.BS_USER_NAME,
		"accessKey" : process.env.BS_ACCESS_KEY
	}}};
    let driver = await new webdriver.Builder().usingServer(`https://${process.env.BS_USER_NAME}:${process.env.BS_ACCESS_KEY}@hub-cloud.browserstack.com/wd/hub`).withCapabilities(capabilities).build();
    let stop = !1;
    let ErrorMessage=async function(message,...data){
        await driver.takeScreenshot();
        await driver.executeScript(`browserstack_executor: ${JSON.stringify({
            action: "setSessionStatus",
            arguments: {
                status:"failed",
                reason: message
            }
        })}`);
        await driver.close().catch(e=>console.log(e));
        await driver.quit().catch(e=>console.log(e));
        subscription.unsubscribe();
        stop=!0;
        log('Quiting:', {message,data});
        return {message, data};
    };
    let SuccessMessage = async function(message,...data){
        await driver.executeScript(`browserstack_executor: ${JSON.stringify({
            action: "setSessionStatus",
            arguments: {
                status:"passed",
                reason: message
            }
        })}`);
        await driver.close().catch(e=>console.log(e));
        await driver.quit().catch(e=>console.log(e));
        subscription.unsubscribe();
        stop=!0;
        log('Quiting:', {message,data});
        return {message, data};
    };
    let ConsoleLog=async function(message) {
        log(message);
        return driver.executeScript(`browserstack_executor: ${JSON.stringify({
            action: "annotate",
            arguments: {
                data: message,
                level: "info"
            }
        })}`);
    }
    let subscription = subject.subscribe({
        complete: ErrorMessage.bind(this,'Task cancelled')
    });
	await driver.manage().setTimeouts({ implicit: delay }).catch(e=>console.log(e));
    await driver.manage().window().maximize();
    await driver.get(url).catch(e=>console.log(e));
    let clickPath=async (clickPath,dump)=>{
        for (let target of clickPath) {
            let iframe=null;
            if (/\(sleep\)/.test(target)) {
                await new Promise(r=>setTimeout(r,delay));
                continue;
            }
            if (/\(alert\)/.test(target)) {
                try {
                    await driver.switchTo().alert().dismiss().catch(e=>console.log(e));
                    await driver.switchTo().defaultContent().catch(e=>console.log(e));
                } catch (e) {console.log(e)}
                continue;
            }
            let current_url=await driver.getCurrentUrl().catch(e=>console.log(e));
            await ConsoleLog(`current URL: ${current_url}`);
            if (/\/password/.test(current_url)) throw await ErrorMessage(`Website locked ${current_url}`);
            if (/\(iframe\)/.test(target)){
                let split=target.split("(iframe)");
                iframe = await driver.wait(driver.findElement(webdriver.By.css(split[0].trim())),delay).catch(e=>console.log(e));
                if (!iframe) {
                    await ConsoleLog(`Skipping not found element: ${target}`).catch(e=>console.log(e));
                    continue;
                }
                await driver.switchTo().frame(iframe).catch(e=>console.log(e));
                target=split[1];
            }
            let shadowRoot=null;
            if (/\(shadowRoot\)/.test(target)) {
                let split = target.split("(shadowRoot)");
                shadowRoot = await driver.wait(driver.findElement(webdriver.By.css(split[0].trim())),delay).catch(e=>console.log(e));
                if (!shadowRoot) {
                    await ConsoleLog(`Skipping not found element: ${target}`).catch(e=>console.log(e));
                    continue;
                }
                target=split[1];
            }
            let elements = shadowRoot?
                await driver.executeScript('return [...arguments[0].shadowRoot.querySelectorAll(arguments[1])]',shadowRoot,target).catch(e=>console.log(e))
                :await driver.wait(driver.findElements(webdriver.By.css(target)),delay).catch(e=>console.log(e));
            if (!elements?.length) {
                await ConsoleLog(`Skipping not found element: ${target}`).catch(e=>console.log(e));
                if (iframe) await driver.switchTo().defaultContent().catch(e=>console.log(e));
                continue;
            }
            let clicked=!1;
            for (let element of elements) {
                try {
                    if (!iframe) await driver.actions().scroll(0, 0, 0, Math.round(60+(Math.random()*10)), element).perform().catch(e=>console.log(e));
                    await ConsoleLog(`${dump? 'Dumping':'Clicking'} "${await element.getText().catch(e=>console.log(e))}"\nselector ${target}`);
                    dump? dump(current_url, element.getAttribute('outerHTML')):await element.click();
                    clicked=!dump;
                } catch (e) {}
            }
            if (iframe) await driver.switchTo().defaultContent().catch(e=>console.log(e));
            clicked && await new Promise(r=>setTimeout(r,delay));
        }
    }
    while (repeat--){
        if (stop) break;
        await clickPath(clickPathBefore);
        await clickPath(dump, (url,content)=>!!(content||'').length||subject.next({url,content}));
        await clickPath(clickPathAfter);
    }
    return await SuccessMessage('Finished');
}
module.exports = {scrapers}