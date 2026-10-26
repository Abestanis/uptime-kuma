const { R } = require("redbean-node");
const { getRandomInt, log, sleep } = require("../src/util");
const { UptimeKumaServer } = require("./uptime-kuma-server");


/**
 * Check if a given user owns a specific monitor
 * @param {number} userID ID of user to check
 * @param {number} monitorID ID of monitor to check
 * @returns {Promise<void>}
 * @throws {Error} The specified user does not own the monitor
 */
async function checkOwner(userID, monitorID) {
    let row = await R.getRow("SELECT id FROM monitor WHERE id = ? AND user_id = ? ", [monitorID, userID]);

    if (!row) {
        throw new Error("You do not own this monitor.");
    }
}

/**
 * Start the specified monitor
 * @param {number|undefined} userID ID of user who owns monitor
 * @param {number} monitorID ID of monitor to start
 * @returns {Promise<void>}
 */
async function startMonitor(userID, monitorID) {
    log.info("manage", `Resume Monitor: ${monitorID} User ID: ${userID}`);

    if (userID !== undefined) {
        await checkOwner(userID, monitorID);
        await R.exec("UPDATE monitor SET active = 1 WHERE id = ? AND user_id = ? ", [monitorID, userID]);
    } else {
        await R.exec("UPDATE monitor SET active = 1 WHERE id = ? ", [monitorID]);
    }

    let monitor = await R.findOne("monitor", " id = ? ", [monitorID]);

    const server = UptimeKumaServer.getInstance();
    if (monitor.id in server.monitorList) {
        await server.monitorList[monitor.id].stop();
    }

    server.monitorList[monitor.id] = monitor;
    await monitor.start(server.io);
}

/**
 * Restart a given monitor
 * @param {number|undefined} userID ID of user who owns monitor
 * @param {number} monitorID ID of monitor to start
 * @returns {Promise<void>}
 */
async function restartMonitor(userID, monitorID) {
    return await startMonitor(userID, monitorID);
}

/**
 * Pause a given monitor
 * @param {number} userID ID of user who owns monitor
 * @param {number} monitorID ID of monitor to start
 * @returns {Promise<void>}
 */
async function pauseMonitor(userID, monitorID) {
    await checkOwner(userID, monitorID);

    log.info("manage", `Pause Monitor: ${monitorID} User ID: ${userID}`);

    await R.exec("UPDATE monitor SET active = 0 WHERE id = ? AND user_id = ? ", [monitorID, userID]);

    const server = UptimeKumaServer.getInstance();
    if (monitorID in server.monitorList) {
        await server.monitorList[monitorID].stop();
        server.monitorList[monitorID].active = 0;
    }
}

/**
 * Resume active monitors
 * @returns {Promise<void>}
 */
async function startMonitors() {
    let list = await R.find("monitor", " active = 1 ");

    const server = UptimeKumaServer.getInstance();
    for (let monitor of list) {
        server.monitorList[monitor.id] = monitor;
    }

    for (let monitor of list) {
        try {
            await monitor.start(server.io);
        } catch (e) {
            log.error("monitor", e);
        }
        // Give some delays, so all monitors won't make request at the same moment when just start the server.
        await sleep(getRandomInt(300, 1000));
    }
}

module.exports = {
    startMonitor,
    restartMonitor,
    pauseMonitor,
    startMonitors
};
