'use strict';

/**
 * @typedef States
 * @type {object}
 * @property {import('./connectionManager')} ConnectionManager
 * @property {import('./channelManager')} ChannelManager
 * @property {import('./subscriptionManager')} SubscriptionManager
 */

/** @type {States} */
const states = {
    ConnectionManager: require('./connectionManager'),
    ChannelManager: require('./channelManager'),
    SubscriptionManager: require('./subscriptionManager')
};

module.exports = states;
