'use strict';

class NoRouteKeyError extends Error {
    constructor() {
        super('no routekey found');
        this.name = this.constructor.name;
    }
}

module.exports = NoRouteKeyError;
