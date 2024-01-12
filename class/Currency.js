const mysql = require('mysql2/promise');

const util = require('../util');
const ERR = require('../config/error');
const config = {
    mysql: require('../config/mysql')
};

class Currency {
    constructor () {
        this.listByID = new Map();
        this.listByName = new Map();
        this.mysqlConn = null;
    }

    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }
    
    async get (key) {
        const list = (isNaN(key)) ? this.listByName : this.listByID;
        key = (key && isNaN(key)) ? key.toUpperCase() : Number(key);

        if (!list.has(key)) {
            throw ERR.PARAM_MISS;
        }

        return list.get(key);
    }

    async exchange (key) {
        const set = await this.get(key);

        return set.exchangeRate;
    }

    async show () {
        util.log(this.listByID);
        return;
    }

    async update () {
        await this.conn();
        
        const sql = 'select * from game_manage.rp_currency where agent = 0;'
        const [results] = await this.mysqlConn.query(sql);

        for (const row of results) {
            this.listByID.set(Number(row.id), row);
            this.listByName.set(row.currency.toUpperCase(), row);
        }
        
        if (this.mysqlConn) {
            await this.mysqlConn.end();
        }
        return;

    }
}

module.exports = Currency;