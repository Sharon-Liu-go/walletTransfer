
const mysql = require('mysql2/promise');
const Redis = require('ioredis');
const util = require('../util');

const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};


const fs = require('fs');
const readline = require('readline');

class walletTransfer_check {
    constructor() {
        this.mysqlConn;
        this.redis = new Redis(config.redis);
        this.caseSensitiveAccountsRidNotInDBCounts = 0
        this.caseSensitiveAccountsRidNotInDB = {} //未儲存在DB的大小寫帳號(rid數)
        this.allCaseSensitiveAccounts = new Set() //所有大小寫帳號 (去重)
        this.caseSensitiveAccounts_totallySame_ridCounts = 0
        this.caseSensitiveAccounts_totallySame = new Set()  //完全相同大小寫帳號(去重)
        this.caseSensitiveAccounts_totallySame_allRid = {} //完全相同大小寫帳號(在DB或不在DB的所有rid)
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

    async removeFileIfExist(filePath) {
        if (fs.existsSync(filePath)) {
            console.log(`檔案${filePath}已存在，進行刪除`);
            fs.unlinkSync(filePath);
            console.log(`檔案${filePath}已成功刪除`);
        }
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
            let inputFolder = "";
            let outputFolder = "";

            if (this.action === 'PRE') {
                inputFolder = process.env.WALLET_CHECK_EXPORT_PRE_FOLDER;
                outputFolder = process.env.WALLET_CHECK_REPORT_PRE_FOLDER;
            } else if (this.action === 'UPDATE') {
                inputFolder = process.env.WALLET_CHECK_EXPORT_UPDATE_FOLDER;
                outputFolder = process.env.WALLET_CHECK_REPORT_UPDATE_FOLDER;
            } else {
                console.log(`請於.env設定本次執行行為: ACTION=PRE -> 提前 or ACTION=UPDATE -> 當天`)
                return;
            }
            //建立output 的資料夾
            if (!fs.existsSync(outputFolder)) {
              fs.mkdirSync(outputFolder)
            }
            
            const inputFilePath = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SOURCE_PATH;

            await this.removeFileIfExist(inputFilePath);

            //資料來源是 export failAndNoNeedreTry/batchHgetAllValue_account_repeat 和 warningStatusAnylasis/repeatAccounts.json 合併
            const sourceFile1 = inputFolder + 'failAndNoNeedreTry/batchHgetAllValue_account_repeat.json'
            const sourceFile2 = inputFolder + 'warningStatusAnylasis/repeatAccounts.json'
            const files = []
            if (!fs.existsSync(sourceFile1)) {
                console.log(`[本次]檢驗資料檔案來源不存在:${sourceFile1}`)
            } else {
                files.push(sourceFile1)    
            }
            
            if (!fs.existsSync(sourceFile2)) {
                console.log(`[本次]檢驗資料檔案來源不存在:${sourceFile2}`)
            } else {
                files.push(sourceFile2)    
            }
                     
            for (const file of files) {
                const data = fs.readFileSync(file, 'utf8');
                await fs.promises.appendFile(inputFilePath, data, 'utf8');
            }

            if (!fs.existsSync(inputFilePath)) {
                console.log(`[本次]檢驗合併資料檔案來源不存在:${inputFilePath},此次沒有大小寫帳號問題`) 
                return;
            }

            const outputFilePath_inDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_OUTPUT_PATH;
            const outputFilePath_notInDB = outputFolder+ process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH;
            const outputFilePath_summary = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SUMMARY_OUTPUT_PATH;
            const outputFilePath_errLog = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_ERRLOG_OUTPUT_PATH;
            const outputFilePath_totallSameAccounts  = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_TOTALLYSAMEACCOUNTS_OUTPUT_PATH;

            console.log('outputFilePath_inDB: ', outputFilePath_inDB)
            console.log('outputFilePath_notInDB: ', outputFilePath_notInDB)
            console.log('outputFilePath_summary: ', outputFilePath_summary)
            console.log('outputFilePath_errLog: ', outputFilePath_errLog)
            console.log('outputFilePath_totallSameAccounts: ', outputFilePath_totallSameAccounts)

            const outputStream_inDB = fs.createWriteStream(outputFilePath_inDB);
            const outputStream_notInDB = fs.createWriteStream(outputFilePath_notInDB);
            const outputStream_summary = fs.createWriteStream(outputFilePath_summary);
            const outputStream_errLog = fs.createWriteStream(outputFilePath_errLog);
            const outputStream_totallSameAccounts = fs.createWriteStream(outputFilePath_totallSameAccounts);


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
    
                let _obj = {}
                _obj[ridAndPayload.rid] = ridAndPayload.v 
                let objExists = this.caseSensitiveAccounts_totallySame_allRid[obj.account] ? this.caseSensitiveAccounts_totallySame_allRid[obj.account].noInsert : {}
                let accountRids = Object.assign(objExists, _obj);
                this.caseSensitiveAccounts_totallySame_allRid[obj.account] = { noInsert: accountRids };

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

                        let _obj = {}
                        _obj[e.rid] = JSON.parse(e.payload);
                        this.caseSensitiveAccounts_totallySame_allRid[e.account].inserted = _obj;
                        return;
                    }
                    delete this.caseSensitiveAccounts_totallySame_allRid[e.account];
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
                            if (!this.caseSensitiveAccounts_totallySame_allRid[account].inserted[result.rid]) {
                                const sql_player = `INSERT INTO wallet.players (name, currency, money) VALUES (?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),currency=VALUES(currency),money=VALUES(money),updateDate=null;`; 
                                const sql_playerInfo = `INSERT INTO wallet.player_info (account, agent, rid, payload) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE account=VALUES(account),agent=VALUES(agent),rid=VALUES(rid),payload=VALUES(payload),update_time=CURRENT_TIMESTAMP;`; 
                                
                                const [row] = await this.mysqlConn.query('SELECT a.id, b.currency, b.exchangeRate FROM KYDB_NEW.agent a left join game_manage.rp_currency b on a.moneyType = b.id WHERE a.id = ?;', [value.platformId]);                      
                                console.log(`有重複相同帳號的[${account}]將進行以Redis Accounts 對應的rid${result.rid} 重新insert DB : 1.取得agent資訊:${JSON.stringify(row)}`)

                                await Promise.all([this.mysqlConn.query(sql_player, [account, row[0].currency, Math.floor(value.gold / row[0].exchangeRate, 0)]), this.mysqlConn.query(sql_playerInfo, [account, parseInt(value.platformId), parseInt(result.rid), JSON.stringify(value)])])
                                console.log(`有重複相同帳號的[${account}]將進行以Redis Accounts 對應的rid${result.rid} 重新insert DB : 2.成功insertDB\n`);
                                outputStream_totallSameAccounts.write(`有重複相同帳號的[${account}] 原先在跑資料轉移是儲存rid:[${result.rid}]存在DB , 後以Redis Accounts 對應的rid${result.rid} 重新insert DB，insert成功\n`)
                                this.caseSensitiveAccounts_totallySame_allRid[account].noInsert = Object.assign(this.caseSensitiveAccounts_totallySame_allRid[account].noInsert, this.caseSensitiveAccounts_totallySame_allRid[account].inserted)
                                delete this.caseSensitiveAccounts_totallySame_allRid[account].noInsert[result.rid]

                                let _obj = {}
                                _obj[result.rid] = value;
                                this.caseSensitiveAccounts_totallySame_allRid[account].inserted = _obj;
                            }                           
                            this.caseSensitiveAccounts_totallySame_insertedDB++
                            outputStream_inDB.write(`[已儲存在DB的大小寫帳號-有完全相同帳號，已找到正確rid並insert DB]-----${account}----${JSON.stringify({ value: { rid: result.rid, v: value } })}\n`)
                            continue;
                        }
                        this.caseSensitiveAccounts_totallySame_notInsertedDB++
                        outputStream_notInDB.write(`[未儲存在DB的大小寫帳號-有完全相同帳號，已找到正確rid]-----${account}----${JSON.stringify({ value: { rid: result.rid, v: value } })}\n`)
                        continue;
                    }
                    delete this.caseSensitiveAccounts_totallySame_allRid[account];
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

                outputStream_totallSameAccounts.write(JSON.stringify(this.caseSensitiveAccounts_totallySame_allRid))

                if (process.env.ACTION === 'UPDATE') {
                    console.log('將整理出當天全部大小寫帳號玩家:已儲存在DB的玩家 和 未儲存在DB的玩家')
                    const report_pre = process.env.WALLET_CHECK_REPORT_PRE_FOLDER;
                    const input_pre_notInDB = report_pre + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH
                    const input_pre_inDB = report_pre + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_OUTPUT_PATH
                    const input_update_notInDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_OUTPUT_PATH;
                    const output_inDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_FINAL_OUTPUT_PATH;
                    const output_notInDB = outputFolder + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_FINAL_OUTPUT_PATH;
                    
                    console.log('report_pre: ', report_pre)
                    console.log('input_pre_notInDB: ', input_pre_notInDB)
                    console.log('input_pre_inDB: ', input_pre_inDB)
                    console.log('input_update_notInDB: ', input_update_notInDB)
                    console.log('output_inDB: ', output_inDB)
                    console.log('output_notInDB: ', output_notInDB)

                    const files = [];
                    if (!fs.existsSync(input_pre_notInDB)) {
                        console.log(`[當天最終]檢驗資料檔案來源不存在:${input_pre_notInDB}`)
                    } else {
                        files.push(input_pre_notInDB)    
                    }
            
                    if (!fs.existsSync(input_pre_inDB)) {
                        console.log(`[當天最終]檢驗資料檔案來源不存在:${input_pre_inDB}`)
                    } else {
                        files.push(input_pre_inDB)    
                    }

                    if (!fs.existsSync(input_update_notInDB)) {
                        console.log(`[當天最終]檢驗資料檔案來源不存在:${input_update_notInDB}`)
                    } else {
                        files.push(input_update_notInDB)    
                    }
                  
                    const outputStream_inDB_final = fs.createWriteStream(output_inDB);
                    const outputStream_notInDB_final = fs.createWriteStream(output_notInDB);

                    const uniqureAccountObj = {};
                    for (const file of files) {
                        const accounObj = await this.readFileLines(file)
                        Object.assign(uniqureAccountObj, accounObj)
                    }

                    console.log("uniqureAccountObj",JSON.stringify(uniqureAccountObj))
                    const accounts = Object.keys(uniqureAccountObj)
                    const [accountsInserted] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account in (?)', [accounts]);
                    
                    console.log("accountsInserted",accountsInserted)
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
                process.exit();
            });

        } catch (err) {
            this.errCounts++
            console.error(`[執行時發生錯誤]----X----${err}`)
        }

    }
}

module.exports = walletTransfer_check;