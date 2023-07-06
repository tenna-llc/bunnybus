'use strict';

const Code = require('@hapi/code');
const Lab = require('@hapi/lab');
const Assertions = require('../assertions');
const BunnyBus = require('../../../lib');

const { describe, before, beforeEach, after, it } = (exports.lab = Lab.script());
const expect = Code.expect;

let instance = undefined;
let connectionManager = undefined;
let connectionContext = undefined;
let channelManager = undefined;
let channelContext = undefined;

describe('BunnyBus', () => {
    before(() => {});

    describe('public methods', () => {
        describe('checkQueue', () => {
            const baseChannelName = 'bunnybus-checkQueue';
            const baseQueueName = 'test-queue';

            beforeEach(async () => {
                instance = new BunnyBus();

                instance.config = BunnyBus.DEFAULT_SERVER_CONFIGURATION;
                connectionManager = instance.connections;
                channelManager = instance.channels;

                channelContext = await instance._autoBuildChannelContext({ channelName: baseChannelName });
                connectionContext = channelContext.connectionContext;

                await channelContext.channel.deleteQueue(baseQueueName);
            });

            after(async () => {
                await channelContext.channel.deleteQueue(baseQueueName);
                await instance.stop();
            });

            it('should be undefined when queue does not exist', async () => {
                await Assertions.autoRecoverChannel(
                    async () => {
                        const result1 = await instance.checkQueue({ name: baseQueueName });
                        const result2 = instance.channels.get(BunnyBus.MANAGEMENT_CHANNEL_NAME());

                        expect(result1).to.be.undefined();
                        expect(result2.channel).to.exist();
                    },
                    connectionContext,
                    channelContext,
                    channelManager
                );
            });

            it('should return queue info when queue does exist', async () => {
                await channelContext.channel.assertQueue(baseQueueName, BunnyBus.DEFAULT_QUEUE_CONFIGURATION);

                await Assertions.autoRecoverChannel(
                    async () => {
                        const result = await instance.checkQueue({ name: baseQueueName });

                        expect(result).to.exist().and.to.be.an.object().and.to.contain({
                            queue: baseQueueName,
                            messageCount: 0,
                            consumerCount: 0
                        });
                    },
                    connectionContext,
                    channelContext,
                    channelManager
                );
            });

            it('should not error when connection does not pre-exist', async () => {
                await channelContext.channel.assertQueue(baseQueueName, BunnyBus.DEFAULT_QUEUE_CONFIGURATION);
                await connectionManager.close(BunnyBus.DEFAULT_CONNECTION_NAME);

                await Assertions.autoRecoverChannel(
                    async () => {
                        await expect(instance.checkQueue({ name: baseQueueName })).to.not.reject();
                    },
                    connectionContext,
                    channelContext,
                    channelManager
                );
            });

            it('should not error when channel does not pre-exist', async () => {
                await channelContext.channel.assertQueue(baseQueueName, BunnyBus.DEFAULT_QUEUE_CONFIGURATION);
                await channelManager.close(BunnyBus.MANAGEMENT_CHANNEL_NAME());

                await Assertions.autoRecoverChannel(
                    async () => {
                        await expect(instance.checkQueue({ name: baseQueueName })).to.not.reject();
                    },
                    connectionContext,
                    channelContext,
                    channelManager
                );
            });

            it('should not leak event listeners', async () => {
                await channelContext.channel.assertQueue(baseQueueName, BunnyBus.DEFAULT_QUEUE_CONFIGURATION);

                const mgmtChannelContext = await instance._autoBuildChannelContext({
                    channelName: BunnyBus.MANAGEMENT_CHANNEL_NAME()
                });
                const promises = [];

                for (let i = 0; i < 15; ++i) promises.push(instance.checkQueue({ name: baseQueueName }));

                await Promise.all(promises);
                const listeners = mgmtChannelContext.rawListeners('amqp.channel.close');
                // should have the main listener
                expect(listeners.length).to.equal(1);
            });
        });
    });
});
