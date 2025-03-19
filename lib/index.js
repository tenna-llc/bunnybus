'use strict';

const EventEmitter = require('events').EventEmitter;
const Hoek = require('@hapi/hoek');
const Helpers = require('./helpers');
const Exceptions = require('./exceptions');
const { ChannelManager, ConnectionManager, SubscriptionManager } = require('./states');
const { SerialDispatcher, PartitionSerialDispatcher, ConcurrentDispatcher } = require('./schedulers');
const { EventLogger } = require('./loggers');

let singleton = undefined;

class BunnyBus extends EventEmitter {
    constructor(config) {
        super();

        this._state = {
            failed: false,
            recoveryLock: {}
        };

        this._subscriptions = new SubscriptionManager();
        this._connections = new ConnectionManager();
        this._channels = new ChannelManager();
        this.logger = new EventLogger(this);
        this._dispatchers = {
            serial: new SerialDispatcher(),
            partitionSerial: new PartitionSerialDispatcher(config),
            concurrent: new ConcurrentDispatcher()
        };
        this._handlerAssignmentLedger = new Map();

        this._subscriptions.on(SubscriptionManager.BLOCKED_EVENT, this._on_subscriptionManager_BLOCKED_EVENT.bind(this));
        this._subscriptions.on(SubscriptionManager.UNBLOCKED_EVENT, this._on_subscriptionManager_UNBLOCKED_EVENT.bind(this));

        if (config) {
            this.config = config;
        }

        return this;
    }

    static Singleton(config) {
        if (!singleton) {
            singleton = new BunnyBus();
        }

        if (config) {
            singleton.config = config;
        }

        return singleton;
    }

    static get DEFAULT_SERVER_CONFIGURATION() {
        return Helpers.defaultConfiguration.server;
    }

    static get DEFAULT_QUEUE_CONFIGURATION() {
        return Helpers.defaultConfiguration.queue;
    }

    static get DEFAULT_EXCHANGE_CONFIGURATION() {
        return Helpers.defaultConfiguration.exchange;
    }

    static get LOG_DEBUG_EVENT() {
        return EventLogger.LOG_DEBUG_EVENT;
    }

    static get LOG_INFO_EVENT() {
        return EventLogger.LOG_INFO_EVENT;
    }

    static get LOG_WARN_EVENT() {
        return EventLogger.LOG_WARN_EVENT;
    }

    static get LOG_ERROR_EVENT() {
        return EventLogger.LOG_ERROR_EVENT;
    }

    static get LOG_FATAL_EVENT() {
        return EventLogger.LOG_FATAL_EVENT;
    }

    static get PUBLISHED_EVENT() {
        return 'bunnybus.published';
    }

    static get SUBSCRIBED_EVENT() {
        return 'bunnybus.subscribed';
    }

    static get UNSUBSCRIBED_EVENT() {
        return 'bunnybus.unsubscribed';
    }

    static get MESSAGE_DISPATCHED_EVENT() {
        return 'bunnybus.message-dispatched';
    }

    static get MESSAGE_ACKED_EVENT() {
        return 'bunnybus.message-acked';
    }

    static get MESSAGE_NACKED_EVENT() {
        return 'bunnybus.message-nacked';
    }

    static get MESSAGE_REJECTED_EVENT() {
        return 'bunnybus.message-rejected';
    }

    static get MESSAGE_REQUEUED_EVENT() {
        return 'bunnybus.message-requeued';
    }

    static get RECOVERING_CONNECTION_EVENT() {
        return 'bunnybus.recovering-connection';
    }

    static get RECOVERED_CONNECTION_EVENT() {
        return 'bunnybus.recovered-connection';
    }

    static get RECOVERING_CHANNEL_EVENT() {
        return 'bunnybus.recovering-channel';
    }

    static get RECOVERED_CHANNEL_EVENT() {
        return 'bunnybus.recovered-channel';
    }

    static get RECOVERY_FAILED_EVENT() {
        return 'bunnybus.recovery-failed';
    }

    static get DEFAULT_CONNECTION_NAME() {
        return 'default';
    }

    static MANAGEMENT_CHANNEL_NAME() {
        return 'admin-channel';
    }

    static QUEUE_CHANNEL_NAME(queue) {
        return `send-${queue}-channel`;
    }

    static PUBLISH_CHANNEL_NAME() {
        return 'publish-channel';
    }

    get config() {
        return this._config || BunnyBus.DEFAULT_SERVER_CONFIGURATION;
    }

    set config(value) {
        this._config = Object.assign({}, this._config || BunnyBus.DEFAULT_SERVER_CONFIGURATION, value);

        this._dispatchers.partitionSerial.config(this._config);
    }

    get subscriptions() {
        return this._subscriptions;
    }

    get connections() {
        return this._connections;
    }

    get channels() {
        return this._channels;
    }

    get logger() {
        return this._logger;
    }

    set logger(value) {
        if (!Helpers.validateLoggerContract(value)) {
            throw new Exceptions.IncompatibleLoggerError();
        }

        this._logger = value;
        // set logger on internal instances
        this._connections.logger = value;
        this._channels.logger = value;
    }

    get healthy() {
        return !this._state.failed && this.connections.healthy && this.channels.healthy;
    }

    get connectionString() {
        return Helpers.createConnectionString(this.config);
    }

    async createExchange({ name, type, options }) {
        if (!this.config.disableExchangeCreate) {
            const channelContext = await this._autoBuildChannelContext({
                channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME()
            });

            //type : (direct, fanout, header, topic)
            return await channelContext.channel.assertExchange(
                name,
                type,
                Object.assign({}, BunnyBus.DEFAULT_EXCHANGE_CONFIGURATION, options)
            );
        }
    }

    async deleteExchange({ name, options }) {
        const channelContext = await this._autoBuildChannelContext({ channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME() });

        return await channelContext.channel.deleteExchange(name, options);
    }

    async checkExchange({ name }) {
        let result;
        // ensure connection with admin channel
        const channelName = BunnyBus.MANAGEMENT_CHANNEL_NAME();
        const channelContext = await this._autoBuildChannelContext({ channelName });

        try {
            result = await channelContext.channel.checkExchange(name);
        } catch (err) {
            if (err.code !== 404) throw err;
        } finally {
            // as per api docs https://amqp-node.github.io/amqplib/channel_api.html#channel_checkExchange
            // checking on non existing exchange kills the channel so we reconnect
            await this._autoBuildChannelContext({ channelName });
        }

        return result;
    }

    async createQueue({ name, options }) {
        if (!this.config.disableQueueCreate) {
            const channelContext = await this._autoBuildChannelContext({
                channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME()
            });

            return await channelContext.channel.assertQueue(name, Object.assign({}, BunnyBus.DEFAULT_QUEUE_CONFIGURATION, options));
        }
    }

    async purgeQueue({ name }) {
        let result = false;
        // ensure connection with admin channel
        const channelName = BunnyBus.MANAGEMENT_CHANNEL_NAME();
        const channelContext = await this._autoBuildChannelContext({ channelName });

        try {
            await channelContext.channel.purgeQueue(name);
            result = true;
        } catch (err) {
            if (err.code !== 404) throw err;
        } finally {
            await this._autoBuildChannelContext({ channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME() });
        }

        return result;
    }

    async deleteQueue({ name, options }) {
        const channelContext = await this._autoBuildChannelContext({ channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME() });

        return await channelContext.channel.deleteQueue(name, options);
    }

    async checkQueue({ name }) {
        let result;
        // ensure connection with admin channel
        const channelName = BunnyBus.MANAGEMENT_CHANNEL_NAME();
        const channelContext = await this._autoBuildChannelContext({ channelName });

        try {
            // ping server for queue existance
            result = await channelContext.channel.checkQueue(name);
        } catch (err) {
            if (err.code !== 404) throw err;
        } finally {
            // as per api docs https://amqp-node.github.io/amqplib/channel_api.html#channel_checkQueue
            // checking on non existing queue kills the channel so we reconnect
            await this._autoBuildChannelContext({ channelName });
        }

        return result;
    }

    async send({ message, queue, options }) {
        const routeKey = Helpers.reduceRouteKey(null, options, message);
        const source = options && options.source;
        const headerOptions = (options && options.headers) || {};

        const convertedMessage = Helpers.convertToBuffer(message);
        const transactionId = options && options.transactionId ? options.transactionId : Helpers.createTransactionId();
        const [channelContext] = await Promise.all([
            this._autoBuildChannelContext({ channelName: BunnyBus.QUEUE_CHANNEL_NAME(queue), queue }),
            this.createQueue({ name: queue })
        ]);

        const headers = {
            transactionId,
            isBuffer: convertedMessage.isBuffer,
            source,
            routeKey,
            createdAt: new Date().toISOString(),
            bunnyBus: Helpers.getPackageData().version,
            ...headerOptions
        };

        const sendOptions = Helpers.buildPublishOrSendOptions(options, headers);

        await channelContext.channel.sendToQueue(queue, convertedMessage.buffer, sendOptions);
        await channelContext.channel.waitForConfirms();
    }

    async get({ queue, options }) {
        const channelContext = await this._autoBuildChannelContext({
            channelName: BunnyBus.QUEUE_CHANNEL_NAME(queue),
            queue
        });

        return await channelContext.channel.get(queue, options);
    }

    async getAll({ queue, handler, options }) {
        const getOptions = options && options.get;
        const channelName = BunnyBus.QUEUE_CHANNEL_NAME(queue);

        let processing = true;

        do {
            const payload = await this.get({ queue, options: getOptions });

            if (payload) {
                const parsedPayload = Helpers.parsePayload(payload);

                // Not currently handling poison message unlike subscribe

                await handler({
                    message: parsedPayload.message,
                    metaData: parsedPayload.metaData,
                    ack: this._ack.bind(this, { payload, channelName, parsedPayload })
                });
            } else {
                processing = false;
            }
        } while (processing);
    }

    async publish({ message, options }) {
        const globalExchange = (options && options.globalExchange) || this.config.globalExchange;
        const routeKey = Helpers.reduceRouteKey(null, options, message);
        const source = options && options.source;
        const headerOptions = (options && options.headers) || {};

        if (!routeKey) {
            throw new Exceptions.NoRouteKeyError();
        }

        const convertedMessage = Helpers.convertToBuffer(message);
        const transactionId = options && options.transactionId ? options.transactionId : Helpers.createTransactionId();
        const [channelContext] = await Promise.all([
            this._autoBuildChannelContext({ channelName: BunnyBus.PUBLISH_CHANNEL_NAME() }),
            this.createExchange({ name: globalExchange, type: 'topic' })
        ]);

        const headers = {
            transactionId,
            isBuffer: convertedMessage.isBuffer,
            source,
            routeKey,
            createdAt: new Date().toISOString(),
            bunnyBus: Helpers.getPackageData().version,
            ...headerOptions
        };

        const publishOptions = Helpers.buildPublishOrSendOptions(options, headers);

        await channelContext.channel.publish(globalExchange, routeKey, convertedMessage.buffer, publishOptions);
        await channelContext.channel.waitForConfirms();

        this.emit(BunnyBus.PUBLISHED_EVENT, publishOptions, message);
    }

    async subscribe({ queue, handlers, options }) {
        if (this.subscriptions.contains(queue)) {
            throw new Exceptions.SubscriptionExistError(queue);
        }

        if (this.subscriptions.isBlocked(queue)) {
            throw new Exceptions.SubscriptionBlockedError(queue);
        }

        this.subscriptions.create(queue, handlers, options);

        const queueOptions = options && options.queue ? options.queue : null;
        const globalExchange = (options && options.globalExchange) || this.config.globalExchange;
        const maxRetryCount = (options && options.maxRetryCount) || this.config.maxRetryCount;
        const validatePublisher = Hoek.reach(options, 'validatePublisher') || this.config.validatePublisher;
        const validateVersion = Hoek.reach(options, 'validateVersion') || this.config.validateVersion;
        const disableQueueBind = Hoek.reach(options, 'disableQueueBind') || this.config.disableQueueBind;
        const rejectUnroutedMessages = Hoek.reach(options, 'rejectUnroutedMessages') || this.config.rejectUnroutedMessages;
        const rejectPoisonMessages = Hoek.reach(options, 'rejectPoisonMessages') || this.config.rejectPoisonMessages;
        const channelName = BunnyBus.QUEUE_CHANNEL_NAME(queue);

        const channelContext = await this._autoBuildChannelContext({ channelName, queue });

        await Promise.all([
            this.createQueue({ name: queue, options: queueOptions }),
            this.createExchange({ name: globalExchange, type: 'topic' })
        ]);

        if (!disableQueueBind) {
            await Promise.all(Object.keys(handlers).map((pattern) => channelContext.channel.bindQueue(queue, globalExchange, pattern)));
        }

        const result = await channelContext.channel.consume(queue, async (payload) => {
            if (payload) {
                const parsedPayload = Helpers.parsePayload(payload);
                const errorQueue = `${queue}_error`;
                const poisonQueue = `${queue}_poison`;

                if (parsedPayload) {
                    const routeKey = Helpers.reduceRouteKey(payload, null, parsedPayload.message);
                    const currentRetryCount = payload.properties.headers.retryCount || -1;

                    const matchedHandlers = Helpers.handlerMatcher(handlers, routeKey);
                    if (parsedPayload && matchedHandlers.length > 0) {
                        // check for `bunnyBus` header first
                        if (
                            validatePublisher &&
                            !(payload.properties && payload.properties.headers && payload.properties.headers.bunnyBus)
                        ) {
                            const reason = 'message not of BunnyBus origin';
                            this.logger.warn(reason);
                            await this._reject({ payload, channelName, errorQueue }, { reason });
                        }
                        // check for `bunnyBus`:<version> semver
                        else if (validatePublisher && validateVersion && !Helpers.isMajorCompatible(payload.properties.headers.bunnyBus)) {
                            const reason = `message came from older bunnyBus version (${payload.properties.headers.bunnyBus})`;
                            this.logger.warn(reason);
                            await this._reject({ payload, channelName, errorQueue }, { reason });
                        } else if (currentRetryCount < maxRetryCount) {
                            matchedHandlers.forEach(async ({ handler, topic }) => {
                                // set consuming queue, matched topic to message metaData
                                Object.assign(parsedPayload.metaData, { queue, topic });

                                this._dispatchers[this.config.dispatchType].push(
                                    queue,
                                    async () => {
                                        this.emit(BunnyBus.MESSAGE_DISPATCHED_EVENT, parsedPayload.metaData, parsedPayload.message);

                                        await handler({
                                            message: parsedPayload.message,
                                            metaData: parsedPayload.metaData,
                                            ack: this._ack.bind(this, { payload, channelName, parsedPayload }),
                                            nack: this._nack.bind(this, { payload, channelName, parsedPayload }),
                                            rej: this._reject.bind(this, { payload, channelName, errorQueue, parsedPayload }),
                                            requeue: this._requeue.bind(this, { payload, channelName, queue, parsedPayload })
                                        });
                                    },
                                    parsedPayload
                                );
                            });
                        } else {
                            const reason = `message passed retry limit of ${maxRetryCount} for routeKey (${routeKey})`;
                            this.logger.warn(reason);
                            await this._reject({ payload, channelName, errorQueue, parsedPayload }, { reason });
                        }
                    } else {
                        const reason = `message consumed with no matching routeKey (${routeKey}) handler`;
                        this.logger.warn(reason);
                        if (rejectUnroutedMessages) {
                            await this._reject({ payload, channelName, errorQueue, parsedPayload }, { reason });
                        } else {
                            // acking this directly to channel so events don't fire
                            await channelContext.channel.ack(payload);
                        }
                    }
                } else {
                    const reason = `corrupted payload content intercepted`;
                    this.logger.warn(reason);
                    if (rejectPoisonMessages) {
                        await this._reject({ payload, channelName, errorQueue: poisonQueue, parsedPayload }, { reason });
                    } else {
                        // acking this directly to channel so events don't fire
                        await channelContext.channel.ack(payload);
                    }
                }
            }
        });

        if (result && result.consumerTag) {
            this.subscriptions.tag(queue, result.consumerTag);
            this.emit(BunnyBus.SUBSCRIBED_EVENT, queue);
        }
    }

    async unsubscribe({ queue, nackMessages = false }) {
        const channelContext = await this._autoBuildChannelContext({ channelName: BunnyBus.QUEUE_CHANNEL_NAME(queue) });

        if (this.subscriptions.contains(queue)) {
            await channelContext.channel.cancel(this.subscriptions.get(queue).consumerTag);
            // requeue unacknowledged messages on this channel
            nackMessages && (await channelContext.channel.recover());
            this.subscriptions.clear(queue);
            this.emit(BunnyBus.UNSUBSCRIBED_EVENT, queue);
        }
    }

    async resubscribe({ queue }) {
        if (
            this.subscriptions.contains(queue, false) &&
            !this.subscriptions.contains(queue, true) &&
            !this.subscriptions.isBlocked(queue)
        ) {
            const { handlers, options } = this.subscriptions.get(queue);
            await this.subscribe({ queue, handlers, options });
        }
    }

    async stop() {
        this.subscriptions.list().map((subscription) => this.subscriptions.remove(subscription.queue));
        await Promise.allSettled(this.channels.list().map((context) => this.channels.remove(context.name)));
        await Promise.allSettled(this.connections.list().map((context) => this.connections.remove(context.name)));
    }

    //options to store calling module, queue name
    async _ack({ payload, channelName, parsedPayload }, options) {
        const channelContext = await this._autoBuildChannelContext({ channelName });

        await channelContext.channel.ack(payload);

        const _parsedPayload = parsedPayload || Helpers.parsePayload(payload) || { metaData: { headers: {} } };
        _parsedPayload.metaData.headers.ackedAt = new Date().toISOString();

        this.emit(BunnyBus.MESSAGE_ACKED_EVENT, _parsedPayload.metaData, _parsedPayload.message);
    }

    /**
     * Nacks message and returns to queue trying to return to original queue postition
     * https://www.rabbitmq.com/confirms.html#consumer-nacks-requeue
     */
    async _nack({ payload, channelName, parsedPayload }) {
        const channelContext = await this._autoBuildChannelContext({ channelName });
        await channelContext.channel.nack(payload);

        const _parsedPayload = parsedPayload || Helpers.parsePayload(payload) || { metaData: { headers: {} } };
        _parsedPayload.metaData.headers.nackedAt = new Date().toISOString();

        this.emit(BunnyBus.MESSAGE_NACKED_EVENT, _parsedPayload.metaData, _parsedPayload.message);
    }

    async _requeue({ payload, channelName, queue, parsedPayload }, options) {
        const channelContext = await this._autoBuildChannelContext({ channelName });

        const routeKey = Helpers.reduceRouteKey(payload, options);

        const headers = {
            transactionId: payload.properties.headers.transactionId,
            isBuffer: payload.properties.headers.isBuffer,
            source: payload.properties.headers.source,
            createdAt: payload.properties.headers.createdAt,
            requeuedAt: new Date().toISOString(),
            retryCount: payload.properties.headers.retryCount || 0,
            bunnyBus: Helpers.getPackageData().version,
            routeKey
        };
        const sendOptions = Helpers.buildPublishOrSendOptions(options, headers);

        ++sendOptions.headers.retryCount;

        await channelContext.channel.sendToQueue(queue, payload.content, sendOptions);
        await channelContext.channel.waitForConfirms();
        await channelContext.channel.ack(payload);

        const _parsedPayload = parsedPayload || Helpers.parsePayload(payload) || { metaData: { headers: {} } };
        Object.assign(_parsedPayload.metaData.headers, headers);

        this.emit(BunnyBus.MESSAGE_REQUEUED_EVENT, _parsedPayload.metaData, _parsedPayload.message);
    }

    async _reject({ payload, channelName, errorQueue, parsedPayload }, options) {
        const channelContext = await this._autoBuildChannelContext({ channelName });

        const queue = Helpers.reduceErrorQueue(Hoek.reach(this.config, 'errorQueue'), errorQueue, Hoek.reach(options, 'errorQueue'));

        const headers = {
            transactionId: payload.properties.headers.transactionId,
            isBuffer: payload.properties.headers.isBuffer,
            source: payload.properties.headers.source,
            createdAt: payload.properties.headers.createdAt,
            requeuedAt: payload.properties.headers.requeuedAt,
            erroredAt: new Date().toISOString(),
            retryCount: payload.properties.headers.retryCount || 0,
            bunnyBus: Helpers.getPackageData().version,
            reason: Hoek.reach(options, 'reason')
        };

        const sendOptions = Helpers.buildPublishOrSendOptions(options, headers);

        await this.createQueue({ name: queue });
        await channelContext.channel.sendToQueue(queue, payload.content, sendOptions);
        await channelContext.channel.waitForConfirms();
        await channelContext.channel.ack(payload);

        const _parsedPayload = parsedPayload || Helpers.parsePayload(payload) || { metaData: { headers: {} } };
        Object.assign(_parsedPayload.metaData.headers, headers);

        this.emit(BunnyBus.MESSAGE_REJECTED_EVENT, _parsedPayload.metaData, _parsedPayload.message);
    }

    async _autoBuildChannelContext({ channelName, queue = null }) {
        let connectionContext = undefined;
        let channelContext = this.channels.get(channelName);

        if (channelContext && channelContext.channel && channelContext.connectionContext.connection) {
            return channelContext;
        }

        const addListener = (subject, eventName, handler) => {
            const key = `${subject.uniqueName}_${eventName}`;
            if (!this._handlerAssignmentLedger.has(key)) {
                this._handlerAssignmentLedger.set(key, handler);
                subject.on(eventName, handler);
            }
        };

        do {
            // ConnectionManager.create() has an invariant guaranteeing that
            // the `connection` property of the `connectionContext` object it
            // returns is always populated at return time. However, the await
            // on its returned promise represents a preemption point where that
            // connection may be invalidated prior to resumption of this calling
            // method. This race condition actually happens quite reliably when
            // an AMQP connection is lost and a number of asynchronous recovery
            // mechanisms are triggered simultaneously in response.

            // For the time being we will work around this by double-checking
            // the return value after this pre-emption point has been passed and
            // retrying if the returned connection was invalidated in the
            // meantime. In the longer term we may want to reconsider the
            // operation of our current connection recovery procedure.

            connectionContext = await this.connections.create({ name: BunnyBus.DEFAULT_CONNECTION_NAME, connectionOptions: this.config });
        } while (connectionContext.connection === undefined);

        addListener(
            connectionContext,
            ConnectionManager.AMQP_CONNECTION_CLOSE_EVENT,
            this._on_connectionManager_AMQP_CONNECTION_CLOSE_EVENT.bind(this)
        );
        addListener(
            connectionContext,
            ConnectionManager.AMQP_CONNECTION_ERROR_EVENT,
            this._on_connectionManager_AMQP_CONNECTION_ERROR_EVENT.bind(this)
        );

        channelContext = await this.channels.create({ name: channelName, queue, connectionContext, channelOptions: this.config });
        addListener(channelContext, ChannelManager.AMQP_CHANNEL_CLOSE_EVENT, this._on_channelManager_AMQP_CHANNEL_CLOSE_EVENT.bind(this));
        addListener(channelContext, ChannelManager.AMQP_CHANNEL_ERROR_EVENT, this._on_channelManager_AMQP_CHANNEL_ERROR_EVENT.bind(this));

        return channelContext;
    }

    /**
     * Connection recovery has failed and this BunnyBus instance
     * will make no further attempts to recover it. Latch a flag
     * that permanently marks this BunnyBus instance as unhealthy.
     * @param {*} err
     */
    _fatalErrorHandler(err) {
        this._state.failed = true;
        err.meta = { msg: `${this.constructor.name} failed to recover, exiting process` };
        this.logger.error(err);
        this.emit(BunnyBus.RECOVERY_FAILED_EVENT, err);
        throw err;
    }

    async _recoverConnection() {
        try {
            this.logger.warn({ message: `${this.constructor.name} connection reconnecting` });
            for (const { name, queue } of this.channels.list()) {
                // recover connection and channel
                await this._autoBuildChannelContext({ channelName: name, ...(queue ? { queue } : {}) });
                // resubscribe
                if (queue) await this._recoverChannel({ channelName: name });
            }

            this.logger.warn({ message: `${this.constructor.name} connection reconnected` });
        } catch (err) {
            this._fatalErrorHandler(err);
        }
    }

    async _recoverChannel({ channelName }) {
        if (!this._state.recoveryLock[channelName]) {
            this._state.recoveryLock[channelName] = true;

            try {
                const { queue } = this.channels.get(channelName);

                this.logger.warn({ message: `${this.constructor.name} reconnecting channel/queue`, channel: channelName, queue });

                if (queue && this.subscriptions.contains(queue, false)) {
                    const { handlers, options } = this.subscriptions.get(queue);

                    if (!this.subscriptions.isBlocked(queue)) {
                        this.subscriptions.clear(queue);
                        await this.subscribe({ queue, handlers, options });

                        this.logger.warn({
                            message: `${this.constructor.name} reconnected/subscribed channel/queue`,
                            channel: channelName,
                            queue
                        });
                    }
                }
            } catch (err) {
                this._fatalErrorHandler(err);
            } finally {
                this._state.recoveryLock[channelName] = false;
            }
        }
    }

    async _on_subscriptionManager_BLOCKED_EVENT(queue) {
        this.logger.warn({ message: `${this.constructor.name} blocked queue`, queue });

        try {
            await this.unsubscribe({ queue });
        } catch (err) {
            err.meta = { message: `${this.constructor.name} blocked event handling failed`, queue };
            this.logger.error(err);
        }
    }

    async _on_subscriptionManager_UNBLOCKED_EVENT(queue) {
        const subscription = this._subscriptions.get(queue);
        this.logger.warn({ message: `${this.constructor.name} unblocking queue`, queue });

        try {
            await this.subscribe({ queue, handlers: subscription.handlers, options: subscription.options });
        } catch (err) {
            err.meta = { message: `${this.constructor.name} unblocked event handling failed`, queue };
            this.logger.error(err);
        }
    }

    async _on_connectionManager_AMQP_CONNECTION_ERROR_EVENT(err, context) {
        err.meta = { message: `${this.constructor.name} connection error`, connection: context.name };
        this.logger.error(err);

        try {
            -this.emit(BunnyBus.RECOVERING_CONNECTION_EVENT, context.name);
            await this._recoverConnection();
            this.emit(BunnyBus.RECOVERED_CONNECTION_EVENT, context.name);
        } catch (err) {
            err.meta = { message: `${this.constructor.name} connection reconnect failed`, connection: context.name };
            this.logger.error(err);
        }
    }

    async _on_connectionManager_AMQP_CONNECTION_CLOSE_EVENT(context) {
        this.logger.warn({ message: `${this.constructor.name} connection closed`, connection: context.name });

        try {
            this.emit(BunnyBus.RECOVERING_CONNECTION_EVENT, context.name);
            await this._recoverConnection();
            this.emit(BunnyBus.RECOVERED_CONNECTION_EVENT, context.name);
        } catch (err) {
            err.meta = { message: `${this.constructor.name} connection reconnect failed`, connection: context.name };
            this.logger.error(err);
        }
    }

    async _on_channelManager_AMQP_CHANNEL_ERROR_EVENT(err, context) {
        err.meta = { message: `${this.constructor.name} channel error`, channel: context.name };
        this.logger.error(err);

        try {
            this.emit(BunnyBus.RECOVERING_CHANNEL_EVENT, context.name);
            await this._recoverChannel({ channelName: context.name });
            this.emit(BunnyBus.RECOVERED_CHANNEL_EVENT, context.name);
        } catch (err) {
            err.meta = { message: `${this.constructor.name} channel reconnect failed`, channel: context.name };
            this.logger.error(err);
        }
    }

    async _on_channelManager_AMQP_CHANNEL_CLOSE_EVENT(context) {
        this.logger.warn({ message: `${this.constructor.name} channel closed`, channel: context.name });

        try {
            this.emit(BunnyBus.RECOVERING_CHANNEL_EVENT, context.name);
            await this._recoverChannel({ channelName: context.name });
            this.emit(BunnyBus.RECOVERED_CHANNEL_EVENT, context.name);
        } catch (err) {
            err.meta = { message: `${this.constructor.name} channel reconnect failed`, channel: context.name };
            this.logger.error(err);
        }
    }
}

module.exports = BunnyBus;
