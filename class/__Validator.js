const Redis = require('ioredis');

const util = require('../util');
const ERR = require('../config/error');
const config = {
    redis: require('../config/redis')
};

class __Validator {
    constructor (pattern, flag) {
        this.redis = new Redis(config.redis);
        this.stream = null;

        // 定义要匹配的键的模式
        this.pattern = pattern; // 替换为您的模式

        this.cursor = '0';
        this.total = 0;
        this.flag = flag || false;

        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;
        this.delay = Number(process.env.DELAY) || 100;

        this.accountsWithNoRid = [];
        this.accountsWithNoRidCnt = 0;

        this.playersWithNoPlatformId = [];
        this.playersWithNoPlatformIdCnt = 0;

        this.playersWithFloat = [];
        this.playersWithFloatCnt = 0;
    }

    async getPlayersByRid (keys) {
        const pipeline = this.redis.pipeline();

        for (const k of keys) {
            pipeline.hgetall(k);
        }

        const results = await pipeline.exec();
        for (const [err, rst] of results) {
            if (!rst.platformId) {
                this.playersWithNoPlatformIdCnt++;
            }

            if (!Number.isInteger(Number(rst.gold))) {
                this.playersWithFloatCnt++;
            }
        }

        return;
    }

    async getAccountsByKey (keys, next) {
        // keys: Account:*[]
        try {
            const pipeline = this.redis.pipeline();
            const acc = [];

            for (const k of keys) {
                pipeline.hgetall(k);
            }

            const results = await pipeline.exec();
            for (const [err, rst] of results) {
                if (!rst.rid) {
                    this.accountsWithNoRidCnt++;
                    continue;
                }

                acc.push(`Player:${rst.rid}`);
            }

            await this.getPlayersByRid(acc);

            await util.sleep(this.delay);
            next();
            return;
        } catch (e) {
            util.error(e.stack);
            next();
            return;
        }
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
                if (this.flag) {
                    this.stream.pause();
                    
                    this.getAccountsByKey(keys, () => {
                        this.stream.resume();
                    });
                }

                this.total += keys.length;
                util.log('Already done: ', this.total,
                    '\nPlayers With No platformId : ', this.playersWithNoPlatformIdCnt,
                    '\nAccounts With No Rid : ', this.accountsWithNoRidCnt,
                    '\nPlayers With Float : ', this.playersWithFloatCnt);
            });
            this.stream.on("end", () => {
                util.log("all keys have been visited");
                next();
                return;
            });
        });
    }

    async exec () {
        util.log('exec')

        await this.scan();
        
        util.log('================================================================');
        util.log('Total: ', this.total);
        util.log('Players With No platformId : ', this.playersWithNoPlatformIdCnt);
        util.log('Accounts With No Rid : ', this.accountsWithNoRidCnt);
        util.log('Players With Float : ', this.playersWithFloatCnt);
        return;
    }
}

module.exports = __Validator;