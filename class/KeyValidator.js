const utils = require('../util');
const KeyCounter = require('./KeyCounter');

class KeyValidator extends KeyCounter {
    constructor () {
        super();

        this.flag = true;
    }

    async valid (keys) {
        for (const key of keys) {
            console.log(key)
            const d = JSON.parse(key);
            utils.log(d);
        }
    }
}

module.exports = KeyValidator;