const Redis = require('ioredis');
const mysql = require('mysql2/promise');

const util = require('../util');
const ERR = require('../config/error');
// const config = {
//     redis: require('../config/redis').redis,
//     //redis1: require('../config/redis').redis1,
//     mysql: require('../config/mysql')
// };

class findLackPlayers {
    constructor (pattern, flag) {
        this.redis = new Redis(config.redis);
        this.redis1 = new Redis(config.redis1);
        this.stream = null;

        this.mysqlConn;

        // 定义要匹配的键的模式
        this.pattern = pattern; // 替换为您的模式
        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;

        this.cursor = '0';
        this.total = 0;
        this.flag = flag || false;
    }


    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }

    async getMysqlAllPlayersRid() {
        // 设置 session 参数
        await this.mysqlConn.query('SET SESSION group_concat_max_len = 4294967295');

        const sql = `select GROUP_CONCAT(rid SEPARATOR ',') rid from wallet.player_info;`
        const [rows] = await this.mysqlConn.query(sql);

        const ridsArray = rows[0].rid.split(',');
        return ridsArray;
    }

    scan() {     
        return new Promise((resolve,reject) => {
            let keys = [];    
            this.stream = this.redis.scanStream({
                match: this.pattern,
                count: this.size,
            });

            this.stream.on("data", (key) => {
                keys = keys.concat(key);
            });
            this.stream.on("end", () => {
                util.save('./export/findAllPlayersRid.json', JSON.stringify(keys));
                util.log("All keys have been visited!!!")
                this.total = keys.length;
                resolve(keys) ;
            });
        });
    }


    /**
     * TODO: 共用
     */
    async valid () { }

    async exec() {
        try {
        util.log('exec')
        await this.conn();
        const rids_mysql = await this.getMysqlAllPlayersRid();
            
        console.time('rids_redis')
        const rids_redis = await this.scan();
        console.timeEnd('rids_redis') 
        util.log('rids_redis total: ' + this.total);
        
        util.log('=============================比對差異中.....===================================');   
        console.time('比對差異')
        let lackCount = 0    
            const values = await Promise.all(rids_redis.map(async (key) => {
            if (!rids_mysql.includes(key.split(':')[1])) {
                    console.log('lackCount',lackCount)
                    lackCount++
                    let value = await this.redis.hgetall(key); // 获取键的值
                    util.save('./export/findLackPlayersRid.json', key);
                    util.save('./export/findLackPlayersV1.json', JSON.stringify(value));
                    return; 
            }

        }));
        console.timeEnd('比對差異')
        console.log('======================比對差異結束==========================================');
        console.log('count of lack rid  : ',lackCount);
        return;
        } catch (err) {
            console.error(err)
        }
        
    }
}

module.exports = findLackPlayers;