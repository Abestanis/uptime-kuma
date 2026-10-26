const { BeanModel } = require("redbean-node/dist/bean-model");
const fsAsync = require("fs").promises;
const path = require("path");
const Database = require("../database");
const { fsExists } = require("../util-server");

class DockerHost extends BeanModel {
    static CertificateFileNameCA = "ca.pem";
    static CertificateFileNameCert = "cert.pem";
    static CertificateFileNameKey = "key.pem";

    /**
     * Returns an object that ready to parse to JSON
     * @returns {object} Object ready to parse
     */
    toJSON() {
        return {
            id: this.id,
            userID: this.user_id,
            dockerDaemon: this.docker_daemon,
            dockerType: this.docker_type,
            name: this.name,
            autoSync: Boolean(this.auto_sync),
        };
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
}

module.exports = DockerHost;
