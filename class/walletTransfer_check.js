
const mysql = require('mysql2/promise');
const Redis = require('ioredis');
const util = require('../util');

const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};


const fs = require('fs');
const readline = require('readline');

class walletTransfer_preset_check {
    constructor() {
        this.mysqlConn;
        this.redis = new Redis(config.redis);
        this.caseSensitiveAccountsRidNotInDBCounts = 0
        this.caseSensitiveAccountsRidNotInDB = {} //未儲存在DB的大小寫帳號(rid數)
        this.allCaseSensitiveAccounts = new Set() //所有大小寫帳號 (去重)
        this.caseSensitiveAccounts_totallySame_ridCounts = 0
        this.caseSensitiveAccounts_totallySame = new Set()  //完全相同大小寫帳號(去重)
        this.caseSensitiveAccountsInDB = new Set() //已儲存在DB的大小寫帳號  (去重)
        this.caseSensitiveAccounts_totallySame_insertedDB = 0
        this.caseSensitiveAccounts_totallySame_notInsertedDB = 0
        this.caseSensitiveAccounts_not_totallySame = 0
        this.errCounts = 0
    }

    async conn() {
        console.log('Mysql連線Config: ', config.mysql)
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
    }

    async readFileLines(filePath) {
    return new Promise((resolve, reject) => {
        const lines = {};
        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
            crlfDelay: Infinity
        });

        rl.on('line', (line) => {
            const lineArray = line.split('----')
            lines[lineArray[1]] = lineArray[2];
        });

        rl.on('close', () => {
            resolve(lines);
        });

        rl.on('error', (err) => {
            reject(err);
        });
    });
} 

    /**
     * TODO: 共用
     */
    async valid() { }

    async exec() {
        try {
            util.log('exec')
            console.time("EXEC")
            this.action = process.env.ACTION;
            let outputFolder = "";

            if (this.action === 'PRE') {
                outputFolder = process.env.WALLET_CHECK_REPORT_PRE_FOLDER;
            } else if (this.action === 'UPDATE') {
                outputFolder = process.env.WALLET_CHECK_REPORT_UPDATE_FOLDER;
            } else {
                console.log(`請於.env設定本次執行行為: ACTION=PRE -> 提前 or ACTION=UPDATE -> 當天`)
                return;
            }
            //建立output 的資料夾
            if (!fs.existsSync(outputFolder)) {
              fs.mkdirSync(outputFolder)
            }

            const inputFilePath = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SOURCE_PATH;
            const outputFilePath_inDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_OUTPUT_PATH;
            const outputFilePath_notInDB = outputFolder+ process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH;
            const outputFilePath_summary = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SUMMARY_OUTPUT_PATH;
            const outputFilePath_errLog = outputFolder +process.env.WALLET_CHECK_REPEATED_ACCOUNTS_ERRLOG_OUTPUT_PATH;

            console.log('outputFilePath_inDB: ', outputFilePath_inDB)
            console.log('outputFilePath_notInDB: ', outputFilePath_notInDB)
            console.log('outputFilePath_summary: ', outputFilePath_summary)
            console.log('outputFilePath_errLog: ', outputFilePath_errLog)

            const outputStream_inDB = fs.createWriteStream(outputFilePath_inDB);
            const outputStream_notInDB = fs.createWriteStream(outputFilePath_notInDB);
            const outputStream_summary = fs.createWriteStream(outputFilePath_summary);
            const outputStream_errLog = fs.createWriteStream(outputFilePath_errLog);

            // 创建逐行读取的接口
            const fileStream  = fs.createReadStream(inputFilePath);
            // 設置編碼爲 utf8。
            const rl = readline.createInterface({
              input: fileStream,
              crlfDelay: Infinity,
            })
            console.log('--------開始處理未儲存DB的大小寫重複帳號資料------------')
            // 处理每一行的逻辑
            rl.on('line', (line) => {
                this.caseSensitiveAccountsRidNotInDBCounts++
                const lineArray = line.split('----')
                const obj = {};
                obj.reason = lineArray[0]
                obj.account = lineArray[1];
                const ridAndPayload = JSON.parse(lineArray[2]).value;
                this.caseSensitiveAccountsRidNotInDB[obj.account] = ridAndPayload;

                if (this.allCaseSensitiveAccounts.has(obj.account)) { //若已存在代表完全一樣的account
                    this.caseSensitiveAccounts_totallySame_ridCounts++
                    this.caseSensitiveAccounts_totallySame.add(obj.account)
                    return;
                }
                this.allCaseSensitiveAccounts.add(obj.account);
            })

            rl.on('error', function (e) {
                console.log(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('close', async () => {
                console.log('--------開始處理已儲存DB的大小寫重複帳號資料------------')
                //取得提前已insert的accounts
                const allNoInsertAccounts = [...this.allCaseSensitiveAccounts]
                await this.conn();
                const [accountsInserted] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account in (?)', [allNoInsertAccounts]);

                accountsInserted.forEach(e=> {
                    if (this.allCaseSensitiveAccounts.has(e.account)) { //若已存在代表完全一樣的account
                        this.caseSensitiveAccountsInDB.add(e.account)
                        this.caseSensitiveAccounts_totallySame_ridCounts++
                        this.caseSensitiveAccounts_totallySame.add(e.account)
                        return;
                    }
                    this.allCaseSensitiveAccounts.add(e.account)
                    outputStream_inDB.write(`[已儲存在DB的大小寫帳號]-----${e.account}----${JSON.stringify({ value: {rid:e.rid,v:e.payload} })}\n`)
                })

                const caseSensitiveAccountsRidNotInDB = Object.keys(this.caseSensitiveAccountsRidNotInDB)

                for (let i = 0; i < caseSensitiveAccountsRidNotInDB.length; i++){
                    let account = caseSensitiveAccountsRidNotInDB[i];
                    if (this.caseSensitiveAccounts_totallySame.has(account)) {
                        //找到正確rid
                        const result = await this.redis.hgetall('Account:' + account);
                        if (Object.keys(result).length === 0 || !result.rid) {
                            this.errCounts++
                            console.error(`[無法透過redis的Account:${account}找到對應的rid]----${account}----${JSON.stringify(result)}`)
                            outputStream_errLog.write(`[無法透過redis的Account:${account}找到對應的rid]----${account}----${JSON.stringify(result)}`)
                            continue;
                        }

                        const value = await this.redis.hgetall('Player:' + result.rid);
                        if (Object.keys(value).length === 0) {
                            this.errCounts++
                            console.error(`[無法透過redis的Player:${result.rid}找到對應的rid]----${account}----${JSON.stringify(value)}`)
                            outputStream_errLog.write(`[無法透過redis的Player:${result.rid}找到對應的rid]----${account}----${JSON.stringify(value)}`)
                            continue;
                        }
                        
                        if (this.caseSensitiveAccountsInDB.has(account)) {
                            //const sql_player = `INSERT INTO wallet.players (name, currency, money) VALUES ? ON DUPLICATE KEY UPDATE name=VALUES(name),currency=VALUES(currency),money=VALUES(money),updateDate=null;`; 
                            //const sql_playerInfo = `INSERT INTO wallet.player_info (account, agent, rid, payload) VALUES ? ON DUPLICATE KEY UPDATE account=VALUES(account),agent=VALUES(agent),rid=VALUES(rid),payload=VALUES(payload),update_time=CURRENT_TIMESTAMP;`; 
                            //wait Promise.all([this.mysqlConn.query(sql_player, [players_values]), this.mysqlConn.query(sql_playerInfo, [player_info_values])])
                            this.caseSensitiveAccounts_totallySame_insertedDB++
                            outputStream_inDB.write(`[已儲存在DB的大小寫帳號-有完全相同帳號，已找到正確rid並insert DB]-----${account}----${JSON.stringify({ value: { rid: result.rid, v: value } })}\n`)
                            continue;
                        }
                        this.caseSensitiveAccounts_totallySame_notInsertedDB++
                        outputStream_notInDB.write(`[未儲存在DB的大小寫帳號-有完全相同帳號，已找到正確rid]-----${account}----${JSON.stringify({ value: { rid: result.rid, v: value } })}\n`)
                        continue;
                    }
                    this.caseSensitiveAccounts_not_totallySame++
                    outputStream_notInDB.write(`[未儲存在DB的大小寫帳號]-----${account}----${JSON.stringify({value:this.caseSensitiveAccountsRidNotInDB[account]})} \n`)
                    continue;

                }

                let summary = `============Report Summary============\n`
                summary += `本次未儲存在DB的大小寫帳號Rid數量: ${this.caseSensitiveAccountsRidNotInDBCounts} (明細參${inputFilePath})\n`
                summary += `   -大小寫帳號-有完全相同帳號之rid數[${this.caseSensitiveAccounts_totallySame_ridCounts}]`
                summary += `    --去重--> 不重複玩家帳號數: ${this.caseSensitiveAccounts_totallySame.size} : 已insertDB(找到正確rid後inserDB):${this.caseSensitiveAccounts_totallySame_insertedDB} + 未insertDB(找到正確rid後寫進紀錄file): ${this.caseSensitiveAccounts_totallySame_notInsertedDB}\n`
                summary += `                                              --> 重複相同帳號之rid數:${ this.caseSensitiveAccounts_totallySame_ridCounts - this.caseSensitiveAccounts_totallySame.size } \n`
                summary += `   -大小寫帳號-沒有完全相同帳號之rid數[${this.caseSensitiveAccounts_not_totallySame}]\n`
                
                if (this.errCounts === 0) {
                    summary += '本次解析成功!' +'\n'
                } else {
                    summary += `本次解析時有發生錯誤,錯誤筆數共${this.errCounts}筆,錯誤詳情請至${outputFilePath_errLog}查看\n`
                }
                outputStream_summary.write(summary)
                console.log(summary)

                if (process.env.ACTION === 'UPDATE') {
                    const report_pre = process.env.WALLET_CHECK_REPORT_PRE_FOLDER;
                    const input_pre_notInDB = report_pre + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH
                    const input_pre_inDB = report_pre + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_OUTPUT_PATH
                    const input_update_notInDB = outputFolder+ process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH;
                    const output_inDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_FINAL_OUTPUT_PATH;
                    const output_notInDB = outputFolder+ process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB__FINAL_OUTPUT_PATH;
                    const outputStream_inDB_final = fs.createWriteStream(output_inDB);
                    const outputStream_notInDB_final = fs.createWriteStream(output_notInDB);
                    const files = [input_pre_notInDB, input_pre_inDB, input_update_notInDB];
                     const uniqureAccountObj = {};
                    for (const file of files) {
                        const accounObj = await this.readFileLine(file)
                        Object.assign(uniqureAccountObj, accounObj)
                    }

                    const accounts = Object.keys(uniqureAccountObj)
                    const [accountsInserted] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account in (?)', [accounts]);
                    const caseSensitiveAccountsInDB = new Set()
                    accountsInserted.forEach(e => {
                        caseSensitiveAccountsInDB.add(e.account)
                        outputStream_inDB_final.write(`[已儲存在DB的大小寫帳號]-----${e.account}----${JSON.stringify({ value: {rid:e.rid,v:e.payload} })}\n`)
                        delete uniqureAccountObj[e.account]
                    })
                    for (const account in uniqureAccountObj) {
                        outputStream_notInDB_final.write(`[未儲存在DB的大小寫帳號]-----${account}----${JSON.stringify({ value: uniqureAccountObj[account] })} \n`)
                    }
                }
                console.timeEnd("EXEC")
            });

        } catch (err) {
            this.errCounts++
            console.error(`[執行時發生錯誤]----X----${err}`)
            outputStream_errLog.write(`[執行時發生錯誤]----X----${err}`)
        }

    }
}

module.exports = walletTransfer_preset_check;