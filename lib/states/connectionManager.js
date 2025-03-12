'use strict';

const EventEmitter = require('events').EventEmitter;
const Amqp = require('amqplib');
const { isFatalError } = require('amqplib/lib/connection');
const Helpers = require('../helpers');

class Connection extends EventEmitter {
    constructor({ name, connectionOptions, socketOptions }) {
        super();

        this._createdAt = Date.now();
        this._name = name;
        this._connectionOptions = connectionOptions;
        this._socketOptions = socketOptions;
        this._lock = false;
        this._blocked = false;
        this._connection = undefined;

        return this;
    }

    get name() {
        return this._name;
    }

    get uniqueName() {
        return `connection_${this._name}_${this._createdAt}`;
    }

    get connectionOptions() {
        return this._connectionOptions;
    }

    get socketOptions() {
        return this._socketOptions;
    }

    get lock() {
        return this._lock;
    }

    set lock(value) {
        this._lock = value;
    }

    get blocked() {
        return this._blocked;
    }

    set blocked(value) {
        this._blocked = value;
    }

    get connection() {
        return this._connection;
    }

    set connection(value) {
        this._connection = value;
    }

    get healthy() {
        const lockTimeDiff = Date.now() - (this.lock || Date.now());
        const overTimeLimit = lockTimeDiff > this.connectionOptions.timeout * this.connectionOptions.connectionRetryCount;

        return overTimeLimit ? Boolean(this.connection) : true;
    }
}

class ConnectionManager extends EventEmitter {
    constructor({ logger } = {}) {
        super();

        this._connections = new Map();
        this._logger = logger;
        this._loggerMessage = `BunnyBus ${this.constructor.name}`;

        return this;
    }

    static get AMQP_CONNECTION_ERROR_EVENT() {
        return 'amqp.connection.error';
    }

    static get AMQP_CONNECTION_CLOSE_EVENT() {
        return 'amqp.connection.close';
    }

    static get AMQP_CONNECTION_BLOCKED_EVENT() {
        return 'amqp.connection.blocked';
    }

    static get AMQP_CONNECTION_UNBLOCKED_EVENT() {
        return 'amqp.connection.unblocked';
    }

    static get CONNECTION_REMOVED() {
        return 'connectionManager.removed';
    }

    get healthy() {
        for (const context of this.list()) {
            if (!context.healthy) {
                return false;
            }
        }

        return true;
    }

    get logger() {
        if (this._logger) return this._logger;

        return {
            info: (...args) => console.log(...args),
            debug: (...args) => console.log(...args),
            error: (...args) => console.error(...args),
            warn: (...args) => console.log(...args),
            fatal: (...args) => console.error(...args)
        };
    }

    set logger(value) {
        this._logger = value;
    }

    async create({ name, connectionOptions, socketOptions }) {
        if (!connectionOptions) {
            throw new Error('Expected connectionOptions to be supplied');
        }

        const isNew = !this._connections.has(name);

        const connectionContext = !isNew ? this._connections.get(name) : new Connection({ name, connectionOptions, socketOptions });

        if (connectionContext.connection) {
            return connectionContext;
        }

        if (isNew) {
            this._connections.set(name, connectionContext);
        }

        if (connectionContext.lock) {
            await Helpers.intervalAsync(async () => !connectionContext.lock && connectionContext.connection);
        } else {
            connectionContext.lock = Date.now();

            try {
                this.logger.warn({ message: `${this._loggerMessage} connection attempt`, name });
                await Helpers.retryAsync({
                    asyncFunc: async () => {
                        connectionContext.connection = await Helpers.timeoutAsync(
                            Amqp.connect,
                            connectionContext.connectionOptions.timeout
                        )(connectionContext.connectionOptions, connectionContext.socketOptions);
                        connectionContext.connection
                            .on('close', (err, ...args) => {
                                connectionContext.connection = undefined;
                                connectionContext.emit(ConnectionManager.AMQP_CONNECTION_CLOSE_EVENT, connectionContext);

                                if (err) {
                                    err.meta = {
                                        message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_CLOSE_EVENT}`,
                                        name,
                                        isFatalError: isFatalError(err)
                                    };
                                    this.logger.error(err);
                                } else {
                                    this.logger.warn({
                                        message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_CLOSE_EVENT}`,
                                        name
                                    });
                                }
                            })
                            .on('error', (err, ...args) => {
                                connectionContext.emit(ConnectionManager.AMQP_CONNECTION_ERROR_EVENT, err, connectionContext);

                                err.meta = {
                                    message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_ERROR_EVENT}`,
                                    name,
                                    isFatalError: isFatalError(err),
                                    ...args
                                };
                                this.logger.error(err);
                            })
                            .on('blocked', (reason, ...args) => {
                                connectionContext.blocked = true;
                                connectionContext.emit(ConnectionManager.AMQP_CONNECTION_BLOCKED_EVENT, connectionContext, reason);

                                this.logger.warn({
                                    message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_BLOCKED_EVENT}`,
                                    name,
                                    reason,
                                    ...args
                                });
                            })
                            .on('unblocked', (...args) => {
                                connectionContext.blocked = false;
                                connectionContext.emit(ConnectionManager.AMQP_CONNECTION_UNBLOCKED_EVENT, connectionContext);

                                this.logger.warn({
                                    message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_UNBLOCKED_EVENT}`,
                                    name,
                                    ...args
                                });
                            });
                    },
                    interval: Helpers.exponentialBackoff,
                    times: connectionOptions.connectionRetryCount,
                    errorFilterFunc: (err) => {
                        err.meta = {
                            message: `${this._loggerMessage} ${ConnectionManager.AMQP_CONNECTION_CLOSE_EVENT}`,
                            name,
                            isFatalError: isFatalError(err)
                        };
                        this.logger.error(err);
                        return err.code && err.code === 'ENOTFOUND';
                    }
                });
                this.logger.warn({ message: `${this._loggerMessage} connection suceeded`, name });
            } catch (err) {
                err.meta = { message: `${this._loggerMessage} connection failed`, name };
                this.logger.error(err);
                this._connections.delete(name);
                throw err;
            } finally {
                connectionContext.lock = undefined;
            }
        }

        return connectionContext;
    }

    contains(name) {
        return this._connections.has(name);
    }

    get(name) {
        return this._connections.get(name);
    }

    list() {
        const results = [];

        for (const [name, context] of this._connections) {
            results.push(context);
        }

        return results;
    }

    hasConnection(name) {
        return this.getConnection(name) ? true : false;
    }

    getConnection(name) {
        return this._connections.has(name) ? this._connections.get(name).connection : undefined;
    }

    async remove(name) {
        if (this._connections.has(name)) {
            const connectionContext = this._connections.get(name);

            await this.close(name);

            this._connections.delete(name);

            connectionContext.emit(ConnectionManager.CONNECTION_REMOVED, connectionContext);
            this.emit(ConnectionManager.CONNECTION_REMOVED, connectionContext);
        }
    }

    async close(name) {
        if (this._connections.has(name)) {
            const connectionContext = this._connections.get(name);

            if (connectionContext.connection) {
                const oldConnection = connectionContext.connection;
                connectionContext.connection = undefined;
                await oldConnection.close();
            }
        }
    }
}

module.exports = ConnectionManager;
