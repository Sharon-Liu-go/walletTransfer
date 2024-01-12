const mysql = require('mysql2/promise');

const util = require('../util');
const ERR = require('../config/error');
const config = {
    mysql: require('../config/mysql')
};

class Agent {
    constructor () {
        this.listByID = new Map();
        this.mysqlConn = null;
    }

    async conn () {
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }
    
    async get (key) {
        return this.listByID.get(key);
    }

    async show () {
        util.log(this.listByID);
        return;
    }

    async update () {
        await this.conn();
        
        const sql = 'select id, moneyType from KYDB_NEW.agent;'
        const [results] = await this.mysqlConn.query(sql);

        for (const row of results) {
            this.listByID.set(Number(row.id), row.moneyType);
        }
        
        if (this.mysqlConn) {
            await this.mysqlConn.end();
        }
        return;

    }
}

module.exports = Agent;