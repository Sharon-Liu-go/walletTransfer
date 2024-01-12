const mysql = require('mysql2/promise');
const moment = require('moment');

const util = require('../util');
const ERR = require('../config/error');
const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql'),
    exchange: require('../config/exchange'),
    ...require('../config/common')
};

const Validator = require('./Valildator');
const Currency = require('./Currency');
const Agent = require('./Agent');

class WalletValidator extends Validator {
    constructor (pattern, checkMysql) {
        super(pattern);

        this.mysqlConn = null;
        this.currency = new Currency();
        this.agents = new Agent();

        this.flag.valid = true;
        this.flag.mysql = checkMysql || false;

        this.accountsWithNoRid = [];
        this.accountsWithNoRidCnt = 0;
        
        this.playerAlreadyUpdate = [];
        this.playerAlreadyUpdateCnt = 0;

        this.playersWithNoPlatformId = [];
        this.playersWithNoPlatformIdCnt = 0;

        this.playersWithFloat = [];
        this.playersWithFloatCnt = 0;

        this.noMatchedWallet = [];
        this.noMatchedWalletCnt = 0;

        this.noMatchedAgent = [];
        this.noMatchedAgentCnt = 0;

        this.noMatchedCurrency = [];
        this.noMatchedCurrencyCnt = 0;
    
        this.noMatchedMoneyCuzFloor = [];
        this.noMatchedMoneyCuzFloorCnt = 0;

        this.noMatchedMoney = [];
        this.noMatchedMoneyCnt = 0;

        this.noMatchedPlayerInfo = [];
        this.noMatchedPlayerInfoCnt = 0;
    }

    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }

    async getWallets (players) {
        const params = [];

        if (!players.length) {
            return [];
        }

        for (const player of players) {
            params.push(player.name);
        }

        const sql = `select * from ${config.schemaTablePlayers} where name in (?);`
        const [results] = await this.mysqlConn.query(sql, [params]);
        return results;
    }

    /*async getPlayerInfos (players) {
        const params = [];

        if (!players.length) {
            return [];
        }

        for (const player of players) {
            params.push(player.name);
        }

        const sql = 'select * from wallet.player_info where account in (?);'
        const [results] = await this.mysqlConn.query(sql, [params]);
        return results;
    }*/

    async compare (players, wallets, infos) {
        for (const player of players) {
            // if (!infos.has(player.name)) {
            //     this.noMatchedPlayerInfoCnt++;
            //     this.noMatchedPlayerInfo.push(player.name);
            //     continue;
            // }
            // const info = infos.get(player.name);

            if (!wallets.has(player.name)) {
                this.noMatchedWalletCnt++;
                this.noMatchedWallet.push(player.name);
                continue;
            }
            const wallet = wallets.get(player.name);

            const currency = await this.agents.get(Number(player.platformId));
            if (!currency) {
                this.noMatchedAgent.push(player.name)
                this.noMatchedAgentCnt++;
                continue;
            }

            const exchangeRate = await this.currency.exchange(currency);
            //const exchangeRate = config.exchange.get(currency);
            
            if (!exchangeRate) {
                this.noMatchedCurrency.push(player.name)
                this.noMatchedCurrencyCnt++;
                continue;
            }

            const walletGold = Math.floor(Number(exchangeRate) * wallet.money);

            if (walletGold !== Number(player.gold)) {
                if (config.allowNumericalError && Math.abs(walletGold - Number(player.gold) < config.allowNumericalRange)) {
                    continue;
                }
                util.save('./export/noMatchedMoney.json', JSON.stringify({
                    name: player.name, 
                    exchangeRate, 
                    wallet: wallet.money, 
                    'wallet*rate': walletGold, 
                    redis: Number(player.gold), 
                    'redis.updateTime': (player.updateTime) ? moment(player.updateTime*1000).format('YYYY-MM-DD HH:mm:ss') : '' }));
                if (Math.abs(walletGold - Number(player.gold)) <= 10) {
                    this.noMatchedMoneyCuzFloorCnt++;
                    continue;
                }

                this.noMatchedMoney.push([player.name, walletGold, Number(player.gold)])
                this.noMatchedMoneyCnt++;
                continue;
            }
        }
    }

    async valid (keys, next) {
        const self = this;
        const accounts = await this.pipHGetAll(keys, (row, listNext) => {
            if (!row.rid) {
                self.accountsWithNoRidCnt++;
                return;
            }

            listNext.push(`Player:${row.rid}`);
        });

        const players = await this.pipHGetAll(accounts, (row, listNext) => {
            if (row.updateTime*1000 > 1703001600000) { // '2023-12-20 00:00:00'
                this.playerAlreadyUpdateCnt++;
                this.playerAlreadyUpdate.push(row.name);
                return;
            }

            if (!row.platformId) {
                this.playersWithNoPlatformIdCnt++;
                this.playersWithNoPlatformId.push(row.name);
                return;
            }

            if (!Number.isInteger(Number(row.gold))) {
                this.playersWithFloatCnt++;
                this.playersWithFloat.push(row.name);
                return;
            }

            listNext.push(row);
        });

        if (!this.flag.mysql) {
            return next();
        }

        const wallets = await this.getWallets(players);
        // console.log(wallets.length);
        // const playerInfos = await this.getPlayerInfos(players);

        // await this.compare(players, util.array2Map(wallets, 'name'), util.array2Map(playerInfos, 'account'));
        await this.compare(players, util.array2Map(wallets, 'name'));

        return next();
    }

    async exec () {
        util.log('exec')
        if (this.flag.mysql) {
            await this.currency.update();
            await this.agents.update();
        }


        await this.conn();
        await this.scan();
        
        util.log('===========================Summary==============================\n');
        util.log(`Total: ${this.total}, Jobs: ${this.job}`);

        util.log('Accounts With No Rid : ', this.accountsWithNoRidCnt);
        util.log('Players With No platformId : ', this.playersWithNoPlatformIdCnt, this.playersWithNoPlatformId);
        util.log('Players With Float : ', this.playersWithFloatCnt, this.playersWithFloat);

        util.log('Player Already Update Cnt : ', this.playerAlreadyUpdateCnt);

        util.log('No Matched Wallet : ', this.noMatchedWalletCnt, /*this.noMatchedWallet*/);
        util.log('No Matched Agent : ', this.noMatchedAgentCnt, this.noMatchedAgent);
        util.log('No Matched Currency : ', this.noMatchedCurrencyCnt, /*this.noMatchedCurrency*/);
        util.log('No Matched Money : ', this.noMatchedMoneyCnt, this.noMatchedMoney);
        util.log('No Matched player_info : ', this.noMatchedPlayerInfoCnt, this.noMatchedPlayerInfo);

        if (this.redis) {
            this.redis.quit();
        }

        if (this.mysqlConn) {
            await this.mysqlConn.end();
        }

        return;
    }
}

module.exports = WalletValidator;