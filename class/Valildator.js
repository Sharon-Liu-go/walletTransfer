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
        this.job = 0;

        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;
        this.delay = Number(process.env.DELAY) || 100;

        this.flag = {
            valid: false
        }
    }

    async pipHGetAll (keys, fn) {
        const pipeline = this.redis.pipeline();
        const listNext = [];

        for (const k of keys) {
            pipeline.hgetall(k);
        }

        const results = await pipeline.exec();
        for (const [err, rst] of results) {
            await fn(rst, listNext);
        }

        return listNext;
    }

    async valid (keys, next) {}

    scan () {
        return new Promise((next) => {
            this.stream = this.redis.scanStream({
                match: this.pattern,
                count: this.size,
            });

            this.stream.on("data", (keys) => {
                if (this.flag.valid) {
                    this.stream.pause();
                    this.valid(keys, () => {
                        this.stream.resume();
                    });
                }

                this.job ++;
                this.total += keys.length;
                util.log(`Already scan: ${this.total}`);
            });

            this.stream.on("end", async () => {
                util.log(`All keys[${this.pattern}] have been visited!!!`);
                await util.sleep(3000)
                next();
                return;
            });
        });
    }

    async exec () {}
}

module.exports = KeyCounter;