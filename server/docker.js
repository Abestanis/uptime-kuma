const axios = require("axios");
const { R } = require("redbean-node");
const https = require("https");
const { axiosAbortSignal } = require("./util-server");
const { setTimeout: sleep } = require('node:timers/promises');
const { restartMonitor, startMonitor } = require("./monitor");
const { UptimeKumaServer } = require("./uptime-kuma-server");
const Monitor = require("./model/monitor");
const DockerHostModel = require("./model/docker_host");
class DockerHost {
    /**
     * Map of Docker host IDs to their respective container change listeners.
     * @type {Map<number, {controller: AbortController, task: Promise<void>}>}
     */
    static containerChangeListeners = new Map();
    /**
     * Indicates whether the server is shutting down. When true, to more event listeners will be started.
     */
    static shuttingDown = false;

    /**
     * Save a docker host
     * @param {object} dockerHost Docker host to save
     * @param {?number} dockerHostID ID of the docker host to update
     * @param {number} userID ID of the user who adds the docker host
     * @returns {Promise<Bean>} Updated docker host
     */
    static async save(dockerHost, dockerHostID, userID) {
        let bean;

        if (dockerHostID) {
            bean = await R.findOne("docker_host", " id = ? AND user_id = ? ", [dockerHostID, userID]);

            if (!bean) {
                throw new Error("docker host not found");
            }
        } else {
            bean = R.dispense("docker_host");
        }

        bean.user_id = userID;
        bean.docker_daemon = dockerHost.dockerDaemon;
        bean.docker_type = dockerHost.dockerType;
        bean.name = dockerHost.name;
        bean.auto_sync = dockerHost.autoSync === true;
        await DockerHost.stopContainerChangeListener(bean.id);

        await R.store(bean);
        if (bean.auto_sync) {
            await DockerHost.startContainerChangeListener(bean, DockerHost.onContainerUpdate);
        }

        return bean;
    }

    /**
     * Delete a Docker host
     * @param {number} dockerHostID ID of the Docker host to delete
     * @param {number} userID ID of the user who created the Docker host
     * @returns {Promise<void>}
     */
    static async delete(dockerHostID, userID) {
        let bean = await R.findOne("docker_host", " id = ? AND user_id = ? ", [dockerHostID, userID]);

        if (!bean) {
            throw new Error("docker host not found");
        }

        // Delete removed proxy from monitors if exists
        await R.exec("UPDATE monitor SET docker_host = null WHERE docker_host = ?", [dockerHostID]);
        if (bean.auto_sync) {
            await DockerHost.stopContainerChangeListener(dockerHostID);
        }
        await R.trash(bean);
    }

    /**
     * Fetches the amount of containers on the Docker host
     * @param {object} dockerHost Docker host to check for
     * @returns {Promise<number>} Total amount of containers on the host
     */
    static async testDockerHost(dockerHost) {
        const options = {
            url: "/containers/json?all=true",
            timeout: 5000,
            headers: {
                Accept: "*/*",
            },
            signal: axiosAbortSignal(6000),
        };

        if (dockerHost.dockerType === "socket") {
            options.socketPath = dockerHost.dockerDaemon;
        } else if (dockerHost.dockerType === "tcp") {
            options.baseURL = DockerHostModel.patchDockerURL(dockerHost.dockerDaemon);
            options.httpsAgent = new https.Agent(
                await DockerHostModel.getHttpsAgentOptions(dockerHost.dockerType, options.baseURL)
            );
        }

        try {
            let res = await axios.request(options);

            if (Array.isArray(res.data)) {
                if (res.data.length > 0) {
                    if ("ImageID" in res.data[0]) {
                        return res.data.length;
                    } else {
                        throw new Error("Invalid Docker response, is it Docker really a daemon?");
                    }
                } else {
                    return res.data.length;
                }
            } else {
                throw new Error("Invalid Docker response, is it Docker really a daemon?");
            }
        } catch (e) {
            if (e.code === "ECONNABORTED" || e.name === "CanceledError") {
                throw new Error("Connection to Docker daemon timed out.");
            } else {
                throw e;
            }
        }
    }

    /**
     * Listen for container changes on the Docker host until cancelled.
     * @param {object} dockerHost Docker host to listen for container changes
     * @param {(dockerHostID: number, containerData: {id: string, name: string|undefined, labels: Record<string, string>}) => Promise<void>} onUpdate Callback function to call when a container has changed
     * @param {AbortSignal} cancelSignal Signal to cancel the operation
     * @returns {Promise<void>} The listener task.
     */
    static async listenForContainerChanges(dockerHost, onUpdate, cancelSignal) {
        const streamOptions = {
            method: "GET",
            url: "/events",
            params: {
                filters: JSON.stringify({
                    type: ["container"],
                    event: ["create", "rename", "update"],
                }),
            },
            responseType: "stream",
            signal: cancelSignal,
        };
        const requestOptions = {
            url: "/containers/json?all=true",
            timeout: 5000,
            headers: {
                Accept: "*/*",
            },
            signal: cancelSignal,
        };

        if (dockerHost.dockerType === "socket") {
            streamOptions.socketPath = dockerHost.dockerDaemon;
            requestOptions.socketPath = dockerHost.dockerDaemon;
        } else if (dockerHost.dockerType === "tcp") {
            const baseURL = DockerHostModel.patchDockerURL(dockerHost.dockerDaemon);
            const httpsAgent = new https.Agent(
                await DockerHostModel.getHttpsAgentOptions(dockerHost.dockerType, baseURL)
            );
            streamOptions.baseURL = baseURL;
            streamOptions.httpsAgent = httpsAgent;
            requestOptions.baseURL = baseURL;
            requestOptions.httpsAgent = httpsAgent;
        } else {
            throw new Error("Unsupported Docker connection type");
        }

        while (!cancelSignal.aborted) {
            try {
                // Start listening for events.
                const stream = await axios.request(streamOptions);
                if (cancelSignal.aborted) {
                    stream.data.destroy();
                } else {
                    cancelSignal.addEventListener("abort", () => { stream.data.destroy(); }, { once: true });
                }

                // Fetch the current list of containers.
                let response = await axios.request(requestOptions);
                if (!Array.isArray(response.data) || (response.data.length > 0 && !("ImageID" in response.data[0]))) {
                    throw new Error(`Invalid Docker response from ${dockerHost.dockerDaemon}, is it Docker really a daemon?`);
                }
                for (const container of response.data) {
                    await onUpdate(dockerHost.id, {
                        id: container.Id,
                        name: container.Names[0]?.replace(/^\//, ''), // Remove leading slash from container name
                        labels: container.Labels || {}
                    }).catch((error) => {
                        console.error(`Error processing container ${container.Id} change on Docker host ${dockerHost.dockerDaemon}:`, error);
                    });
                }

                // Process events from the stream. Each line is a JSON object representing an event.
                let buffer = "";
                for await (const chunk of stream.data) {
                    buffer += chunk.toString("utf8");

                    let newline;
                    while ((newline = buffer.indexOf("\n")) !== -1) {
                        const line = buffer.slice(0, newline).trim();
                        buffer = buffer.slice(newline + 1);
                        if (!line) {
                            continue;
                        }
                        const event = JSON.parse(line);
                        if (event.Type === "container") {
                            await sleep(500, undefined, { signal: cancelSignal }).catch(() => { }); // Wait a bit to ensure the container is fully created before fetching details
                            let response;
                            try {
                                response = await axios.request({ ...requestOptions, url: `/containers/${event.Actor.ID}/json` });
                            } catch (error) {
                                console.debug(`Failed to fetch container ${event.Actor.ID} details on Docker host ${dockerHost.dockerDaemon}:`, error);
                                continue;
                            }
                            await onUpdate(dockerHost.id, {
                                id: response.Id,
                                name: response.Name.replace(/^\//, ''), // Remove leading slash from container name
                                labels: response.Config.Labels || {}
                            }).catch((error) => {
                                console.error(`Error processing container ${container.Id} change on Docker host ${dockerHost.dockerDaemon}:`, error);
                            });
                        }
                    }
                }
                break;
            } catch (error) {
                if (!cancelSignal.aborted) {
                    console.error(`Error while listening for Docker container events on ${dockerHost.dockerDaemon}:`, error);
                    await sleep(5000, undefined, { signal: cancelSignal }).catch(() => { });
                }
            }
        }
    }

    /**
     * Start listening for container changes on the Docker host.
     * @param {object} dockerHost Docker host to listen for container changes
     * @param {(dockerHostID: number, containerData: {id: string, name: string|undefined, labels: Record<string, string>}) => Promise<void>} onUpdate Callback function to call when a container has changed
     * @returns {Promise<void>}
     */
    static async startContainerChangeListener(dockerHost, onUpdate) {
        const id = dockerHost.id;

        while (DockerHost.containerChangeListeners.has(id)) {
            await DockerHost.stopContainerChangeListener(id);
        }
        if (DockerHost.shuttingDown) {
            return;
        }
        const controller = new AbortController();
        const task = DockerHost.listenForContainerChanges(dockerHost, onUpdate, controller.signal).catch((error) => {
            if (!controller.signal.aborted) {
                console.error(`Docker container listener failed for host ${dockerHost.dockerDaemon}:`, error);
            }
        });
        DockerHost.containerChangeListeners.set(id, { controller, task });
    }

    /**
     * Stop listening for container changes on the Docker host.
     * @param {number} dockerHostID ID of the Docker host to stop listening for.
     * @returns {Promise<void>}
     */
    static async stopContainerChangeListener(dockerHostID) {
        const listener = DockerHost.containerChangeListeners.get(dockerHostID);

        if (!listener) {
            return;
        }
        DockerHost.containerChangeListeners.delete(dockerHostID);
        listener.controller.abort();
        await listener.task;
    }

    /**
     * Stop all Docker container listeners.
     * @returns {Promise<void>}
     */
    static async stopAllContainerChangeListeners() {
        const listeners = [...DockerHost.containerChangeListeners.values()];
        DockerHost.containerChangeListeners.clear();

        for (const listener of listeners) {
            listener.controller.abort();
        }
        await Promise.allSettled(listeners.map((listener) => listener.task));
    }


    static async startAllContainerChangeListeners() {
        const hosts = await R.find("docker_host");
        for (const bean of hosts) {
            if (bean.auto_sync) {
                await DockerHost.startContainerChangeListener(bean, DockerHost.onContainerUpdate);
            }
        }

    }

    static async onContainerUpdate(dockerHostID, container) {
        console.error(`onContainerUpdate: ${container}`); // TODO: Remove
        const docker = await R.findOne("docker_host", " id = ? ", [dockerHostID]);
        if (!docker) {
            throw new Error("docker host not found");
        }
        const userID = docker.user_id;
        const containerID = container.name || container.id;
        let bean = await R.findOne("monitor", " type = 'docker' AND docker_container = ? AND docker_host = ? AND user_id = ? ", [containerID, dockerHostID, userID]);
        const newMonitor = !bean;
        if (newMonitor) {
            bean = R.dispense("monitor");
            bean.type = "docker";
            bean.name = containerID;
        }
        bean.user_id = userID;
        bean.docker_container = containerID;
        bean.docker_host = dockerHostID;
        bean.interval = 60;
        bean.retryInterval = 60;

        bean.validate();
        await R.store(bean);

        if (newMonitor) {
            await startMonitor(userID, bean.id);
        } else if (await Monitor.isActive(bean.id, bean.active)) {
            await restartMonitor(userID, bean.id);
        }

        const server = UptimeKumaServer.getInstance();
        for (const socket of server.io.sockets.sockets.values()) {
            if (socket.userID === userID) {
                await server.sendUpdateMonitorIntoList(socket, bean.id);
            }
        }
    }
}

module.exports = {
    DockerHost,
};
