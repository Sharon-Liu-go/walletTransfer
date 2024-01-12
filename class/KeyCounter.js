const Redis = require('ioredis');

const util = require('../util');
const ERR = require('../config/error');
const config = {
    redis: require('../config/redis')
};

class KeyCounter {
    constructor (pattern) {
        this.redis = new Redis(config.redis);
        this.stream = null;

        // 定义要匹配的键的模式
        this.pattern = pattern; // 替换为您的模式

        this.total = 0;

        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;
        this.delay = Number(process.env.DELAY) || 100;
    }

    /**
     * TODO: 共用
     */
    async valid () { }

    scan () {
        return new Promise((next) => {
            this.stream = this.redis.scanStream({
                match: this.pattern,
                count: this.size,
            });

            this.stream.on("data", (keys) => {
                this.total += keys.length;

                util.log('Already done: ', this.total);
            });
            this.stream.on("end", () => {
                util.log("All keys have been visited!!!");
                next();
                return;
            });
        });
    }

    async exec () {
        util.log('exec')

        await this.scan();
        
        util.log('===========================Summary==============================\n');
        util.log(`Key: ${this.pattern}, Total: ${this.total}`);
        return;
    }
}

module.exports = KeyCounter;