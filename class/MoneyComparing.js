const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const util = require('../util')
const ERR = require('../config/error');
const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};

class MoneyComparing {
    constructor () {
        this.redis = new Redis(config.redis);
        this.mysqlConn;
        
        this.total = 0;
        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;
        this.round = 0;
        this.current = 0;
    }
    

    async compare (players) {
        try {
            const pipeline = this.redis.pipeline();

            const players = [
                'Player:214748365334',
                'Player:214748365236',
                'Player:214748365521',
                'Player:214748365691',
                'Player:214748365736'
            ]

            for (const player of players) {
                pipeline.hgetall(player);
            }

            const results = await pipeline.exec();
            for (const [err, rst] of results) {
                console.log(rst);
            }
        } catch (e) {
            util.error(e.stack);
            throw ERR.CONNECTION_ERROR_REDIS;
        }
    }

    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }

    async count () {
        const sql = 'select count(*) total from wallet.players'
        const [rst] = await this.mysqlConn.query(sql);
        this.total = rst.total;
        this.rounds = Math.ceil(this.total / this.size);
        return;
    }

    async query () {
        const sql = 'select * from wallet.players limit ?, ?;'
        const offset = this.current * this.size;
        const limit = this.size;
        const [rst] = await this.mysqlConn.query(sql, [offset, limit]);
        return rst;
    }

    async pipe () {
        // for (const c = 0; c < this.round; c++) {
        for (let c = 0; c < 3; c++) {
            console.time(`Currently on Round ${c}`)

            const players = await this.query();
            const rst = await this.compare(players);

            console.timeEnd(`Currently on Round ${c}`);
        }
    }

    async exec () {
        await this.conn();
        await this.count();
        await this.pipe();

        if (this.mysqlConn) {
            await this.mysqlConn.end();
        }
        return;
    }
}

module.exports = MoneyComparing;