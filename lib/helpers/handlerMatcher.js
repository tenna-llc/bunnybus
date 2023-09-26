'use strict';

const RouteMatcher = require('./routeMatcher');

const handlerMatcher = (handlers, match) => {
    const result = [];

    for (const topic in handlers) {
        if (RouteMatcher(topic, match)) {
            result.push({ handler: handlers[topic], topic });
            break;
        }
    }

    return result;
};

module.exports = handlerMatcher;
