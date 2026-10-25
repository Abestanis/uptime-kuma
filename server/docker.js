const axios = require("axios");
const { R } = require("redbean-node");
const https = require("https");
const fsAsync = require("fs").promises;
const path = require("path");
const Database = require("./database");
const { axiosAbortSignal, fsExists } = require("./util-server");
const { setTimeout: sleep } = require('node:timers/promises');

class DockerHost {
    static CertificateFileNameCA = "ca.pem";
    static CertificateFileNameCert = "cert.pem";
    static CertificateFileNameKey = "key.pem";
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
            bean = await R.findOne("docker_host", " id = ? ", [dockerHostID]);

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
        bean.auto_sync = dockerHost.auto_sync === true;
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
     * @returns {Promise<void>}
     */
    static async delete(dockerHostID) {
        let bean = await R.findOne("docker_host", " id = ? ", [dockerHostID]);

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
            options.baseURL = DockerHost.patchDockerURL(dockerHost.dockerDaemon);
            options.httpsAgent = new https.Agent(
                await DockerHost.getHttpsAgentOptions(dockerHost.dockerType, options.baseURL)
            );
        }

        try {
            let res = await axios.request(options);

            if (Array.isArray(res.data)) {
                if (res.data.length > 1) {
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
     * Since axios 0.27.X, it does not accept `tcp://` protocol.
     * Change it to `http://` on the fly in order to fix it. (https://github.com/louislam/uptime-kuma/issues/2165)
     * @param {any} url URL to fix
     * @returns {any} URL with tcp:// replaced by http://
     */
    static patchDockerURL(url) {
        if (typeof url === "string") {
            // Replace the first occurrence only with g
            return url.replace(/tcp:\/\//g, "http://");
        }
        return url;
    }

    /**
     * Returns HTTPS agent options with client side TLS parameters if certificate files
     * for the given host are available under a predefined directory path.
     *
     * The base path where certificates are looked for can be set with the
     * 'DOCKER_TLS_DIR_PATH' environmental variable or defaults to 'data/docker-tls/'.
     *
     * If a directory in this path exists with a name matching the FQDN of the docker host
     * (e.g. the FQDN of 'https://example.com:2376' is 'example.com' so the directory
     * 'data/docker-tls/example.com/' would be searched for certificate files),
     * then 'ca.pem', 'key.pem' and 'cert.pem' files are included in the agent options.
     * File names can also be overridden via 'DOCKER_TLS_FILE_NAME_(CA|KEY|CERT)'.
     * @param {string} dockerType i.e. "tcp" or "socket"
     * @param {string} url The docker host URL rewritten to https://
     * @returns {Promise<object>} HTTP agent options
     */
    static async getHttpsAgentOptions(dockerType, url) {
        let baseOptions = {
            maxCachedSessions: 0,
            rejectUnauthorized: true,
        };
        let certOptions = {};

        let dirName = new URL(url).hostname;

        let caPath = path.join(Database.dockerTLSDir, dirName, DockerHost.CertificateFileNameCA);
        let certPath = path.join(Database.dockerTLSDir, dirName, DockerHost.CertificateFileNameCert);
        let keyPath = path.join(Database.dockerTLSDir, dirName, DockerHost.CertificateFileNameKey);

        if (
            dockerType === "tcp" &&
            (await fsExists(caPath)) &&
            (await fsExists(certPath)) &&
            (await fsExists(keyPath))
        ) {
            let ca = await fsAsync.readFile(caPath);
            let key = await fsAsync.readFile(keyPath);
            let cert = await fsAsync.readFile(certPath);
            certOptions = {
                ca,
                key,
                cert,
            };
        }

        return {
            ...baseOptions,
            ...certOptions,
        };
    }

    /**
     * Listen for container changes on the Docker host until cancelled.
     * @param {object} dockerHost Docker host to listen for container changes
     * @param {(dockerHostID: number, containerData: any) => Promise<void>} onUpdate Callback function to call when a container has changed
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
            const baseURL = DockerHost.patchDockerURL(dockerHost.dockerDaemon);
            const httpsAgent = new https.Agent(
                await DockerHost.getHttpsAgentOptions(dockerHost.dockerType, baseURL)
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

                // Fetch the current list of containers.
                let response = await axios.request(requestOptions);
                if (!Array.isArray(response.data) || (response.data.length > 0 && !("ImageID" in response.data[0]))) {
                    throw new Error(`Invalid Docker response from ${dockerHost.dockerDaemon}, is it Docker really a daemon?`);
                }
                for (const container of response.data) {
                    await onUpdate(dockerHost.id, container);
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
                            await onUpdate(event.Actor);
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
     * @param {(dockerHostID: number, containerData: any) => Promise<void>} onUpdate Callback function to call when a container has changed
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
            await DockerHost.startContainerChangeListener(bean, DockerHost.onContainerUpdate);
        }

    }

    static async onContainerUpdate(dockerHostID, container) {
        let bean = await R.findOne("docker_host", " id = ? ", [dockerHostID]);
        if (!bean) {
            return;
        }

        console.log(`Container change detected on Docker host ${bean.docker_daemon}:`, container);
    }
}

module.exports = {
    DockerHost,
};
