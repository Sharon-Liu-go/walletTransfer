const Redis = require('ioredis');
const mysql = require('mysql2/promise');
const fs = require('fs');

const util = require('../util');
const ERR = require('../config/error');
const utils = require('../util');
const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};


class walletTransfer_preset {
    constructor(pattern, flag) {
        this.redis = new Redis(config.redis);
        this.stream = null;
        this.mysqlConn;
        // 定义要匹配的键的模式
        this.pattern = pattern; // 替换为您的模式
        this.size = Number(process.env.MAX_PIPE_SIZE) || 100;

        this.cursor = '0';
        this.flag = flag || false;
        this.agentMoneyType = {}

        this.finishRedisScan = false
        this.redisScaned = 0
        this.uniqueAccount = new Set();
        this.failAndNoNeedreTry = 0
        this.affectedRows = 0
        this.insertDuplicates = 0
    }


    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }

    async getAllAgentMoneyType() {
        const [rows] = await this.mysqlConn.query('SELECT a.id, b.currency, b.exchangeRate FROM KYDB_NEW.agent a left join game_manage.rp_currency b on a.moneyType = b.id;');
        rows.forEach(e => { if (e.currency && e.exchangeRate) {
            this.agentMoneyType[e.id] = { currency: e.currency, exchangeRate: e.exchangeRate }
        }})
        util.save('./export_pre/walletTransfer_log.json', JSON.stringify(this.agentMoneyType));
        return
    }

    mkEmptyDir(folderPath) {
        if (!fs.existsSync(folderPath)) {
            fs.mkdirSync(folderPath);
            return;
        }
        //若存在,清空資料夾的檔案     
        fs.readdirSync(folderPath).forEach(file => {
            const curPath = folderPath + '/' + file;
            if (fs.lstatSync(curPath).isDirectory()) { // Recursively delete subfolders
                this.mkEmptyDir(curPath);
            } else { // Delete files
                fs.unlinkSync(curPath);
            }
        });
    }

    scan() {     
        return new Promise((resolve, reject) => {
            console.time('EXEC') 
            console.time('redisScan')
            this.stream = this.redis.scanStream({
                match: this.pattern,
                count: this.size,
            });

            this.stream.on("data", async (key) => {
                this.stream.pause();
                this.redisScaned += key.length;
                console.log('scan data: ', key.length)
                console.time('batch')
                await this.batchHgetAllValue(key)
                console.timeEnd('batch')
                this.stream.resume();
            });
            this.stream.on("end", () => {
                util.log("All keys have been visited!!!")
                util.save('./export_pre/walletTransfer_log.json', `redis Scaned : ${this.redisScaned},成功insert wallet 總筆數: ${this.affectedRows}, 資料有誤總筆數:${this.failAndNoNeedreTry}, insertDuplicates:${this.insertDuplicates}`);
                this.finishRedisScan = true;
                console.timeEnd('redisScan')
            });
       });
    }

    async batchHgetAllValue(keys) {
        const results = await Promise.allSettled(keys.map(async (key) => {
            const result = await this.redis.hgetall(key);
            return { rid : key.split(':')[1] , v : result }; 
        }))
        const players_values = [];
        const player_info_values = [];
        await Promise.allSettled(results.map(async (result) => {
            if (result.status === 'fulfilled') {
                if (!result.value.v.platformId || !result.value.v.accountId || isNaN(result.value.v.gold)) {//gold若是空字串則視為0
                this.failAndNoNeedreTry++
                util.save('./export_pre/failAndNoNeedreTry/batchHgetAllValue_invalidVal.csv', `[no platformId、accountId、gold or gold is NaN]:${JSON.stringify(result)}` )
                return;
                }
                if (result.value.v.accountId.length > 190) {
                    this.failAndNoNeedreTry++
                    util.save('./export_pre/failAndNoNeedreTry/batchHgetAllValue_invalidVal.csv', `[account too log]:${JSON.stringify(result)}`)
                    return;
                }
                if (this.uniqueAccount.has(result.value.v.accountId.toLowerCase().trim())) {
                    this.failAndNoNeedreTry++
                    util.save('./export_pre/failAndNoNeedreTry/batchHgetAllValue_account_repeat.json', `[accountId大小寫重複]----${result.value.v.accountId}----${JSON.stringify(result)}`)
                    return;
                }
                if (!this.agentMoneyType[result.value.v.platformId]) {
                    this.failAndNoNeedreTry++
                    util.save('./export_pre/failAndNoNeedreTry/batchHgetAllValue_invalidVal.csv', `[no agent in agentMoneyTypeMapping]:${JSON.stringify(result)}` )
                    return;
                }
                const goldInRedis = parseFloat(result.value.v.gold);
                players_values.push([result.value.v.accountId, this.agentMoneyType[result.value.v.platformId].currency, Math.floor(goldInRedis/this.agentMoneyType[result.value.v.platformId].exchangeRate,0)]);
                player_info_values.push([result.value.v.accountId, parseInt(result.value.v.platformId), parseInt(result.value.rid), JSON.stringify(result.value.v)])    
                this.uniqueAccount.add(result.value.v.accountId.toLowerCase().trim())
                return;
                }
            this.failAndNoNeedreTry++
            util.save('./export_pre/failAndNoNeedreTry/batchHgetAllValue_fail.json', `${JSON.stringify(result)}`)
        }))
        if (players_values.length === 0) {
            return;
        }
        await this.bashInsertWallet(players_values, player_info_values);
        return;
    }

    async bashInsertWallet(players_values, player_info_values) {
        console.log('start bashInsertWallet')
        const sql_player = `INSERT IGNORE INTO wallet.players (name, currency, money) VALUES ? ;`; 
        const sql_playerInfo = `INSERT IGNORE INTO wallet.player_info (account, agent, rid, payload) VALUES ? ;`; 
        try {
            const result = await Promise.all([await this.mysqlConn.query(sql_player, [players_values]), await this.mysqlConn.query(sql_playerInfo, [player_info_values])])
            this.affectedRows += result[0][0].affectedRows;
            this.insertDuplicates += result[0][0].warningStatus;
            if (result[0][0].warningStatus) {
                 util.save('./export_pre/warningStatus.json', JSON.stringify(player_info_values))
            }

            console.log(`redisScaned: ${this.redisScaned} , 成功insert總筆數: ${this.affectedRows}, 資料有誤總筆數:${this.failAndNoNeedreTry}, insertDuplicates:${this.insertDuplicates}`)
            console.log(`uniqueAccount set:', ${this.uniqueAccount.size}`)

            if (this.finishRedisScan && (this.redisScaned === (this.affectedRows + this.failAndNoNeedreTry + this.insertDuplicates))) {
                console.log('執行完畢,故終止程式!')
                console.timeEnd('EXEC')
                process.exit(); 
            }
            return;
        } catch (err) {
            util.save('./export_pre/bashInsertWallet_errLog.json', err)
            if (players_values.length <= 1) {
                this.failAndNoNeedreTry++
                util.save('./export_pre/failAndNoNeedreTry/bashInsertWallet_failToInsert.json', err + ' ------  ' +  JSON.stringify(players_values) + ',' + JSON.stringify(player_info_values));
                if (this.finishRedisScan && (this.redisScaned === (this.affectedRows + this.failAndNoNeedreTry + this.insertDuplicates))) {
                    console.log(`redis scaned: ${this.redisScaned} , 成功insert wallet 總筆數: ${this.affectedRows}, 資料有誤總筆數:${this.failAndNoNeedreTry}, insertDuplicates:${this.insertDuplicates} 執行完畢,故終止程式!`)
                    console.log('uniqueAccount set:', this.uniqueAccount.size)
                    console.timeEnd('EXEC')
                    process.exit(); 
                }
                return;
            }
            const cutHalf = Math.round(players_values.length / 2);
            this.bashInsertWallet(players_values.slice(0, cutHalf), player_info_values.slice(0, cutHalf));
            this.bashInsertWallet(players_values.slice(cutHalf, players_values.length), player_info_values.slice(cutHalf, player_info_values.length));
            return;
        }

    };

    /**
     * TODO: 共用
     */
    async valid () { }

    async exec() {
        try {
            util.log('exec')
            //建立空的log資料夾
            const export_folder = './export_pre';
            const export_subfolder = './export_pre/failAndNoNeedreTry'
            this.mkEmptyDir(export_folder);
            this.mkEmptyDir(export_subfolder);

            await this.conn();
            console.time('Get All Agent Moenytype Mapping')   
            await this.getAllAgentMoneyType();
            console.timeEnd('Get All Agent Moenytype Mapping') 
            
            await this.scan();

        } catch (err) {
            console.error(err)
        }
        
    }
}

module.exports = walletTransfer_preset;