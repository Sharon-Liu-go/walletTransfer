
const mysql = require('mysql2/promise');
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
        // for report
        this.action = ""
        this.totallySameAccount = new Set() //不重複的相同大小帳號
        this.scanCounts = 0
        this.toallySameAccoutRidCounts =0
        this.allAccounts = {};
        this.group = 0
        this.accountMapGroup = {};
        this.allNoInsertAccounts = []; //for select accounts from mysql -> 拿allAccounts key 就好
        this.caseSensitiveAccountsInDB = 0
    }

    async conn() {
        console.log('Mysql連線Config: ', config.mysql)
        this.mysqlConn = await mysql.createConnection(config.mysql)
        return this.mysqlConn;
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

            if (this.action === 'PRE') {
                
            } else if (this.action === 'TODAY') {
                
            } else {
                console.log(`請於.env設定本次執行行為: ACTION=PRE -> 提前 or ACTION=TODAY -> 當天`)
                return;
            }


            const inputFilePath = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SOURCE_PATH;
            const outputFilePath_notInDB_nameOnly = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_NOT_IN_DB_NAME_ONLY_OUTPUT_PATH;
            const outputFilePath = process.env.WALLET_CHECK_TOTALLY_SAME_ACCOUNTS_OUTPUT_PATH;
            const outputFilePath_nameOnly = process.env.WALLET_CHECK_TOTALLY_SAME_ACCOUNTS_NAME_ONLY_OUTPUT_PATH;
            const outputFilePath_set = process.env.WALLET_CHECK_SET_OUTPUT_PATH;
            const outputFilePath_inDB = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_OUTPUT_PATH;
            const outputFilePath_inDB_nameOnly = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_IN_DB_NAME_ONLY_OUTPUT_PATH;

            console.log('inputFilePath: ', inputFilePath)
            console.log('outputFilePath_notInDB_nameOnly: ', outputFilePath_notInDB_nameOnly)
            console.log('outputFilePath: ', outputFilePath)
            console.log('outputFilePath_nameOnly: ', outputFilePath_nameOnly)
            console.log('outputFilePath_set: ', outputFilePath_set)
            console.log('outputFilePath_inDB: ', outputFilePath_inDB)
            console.log('outputFilePath_inDB_nameOnly: ', outputFilePath_inDB_nameOnly)

            //建立output 的資料夾
            const outputFolder = process.env.WALLET_CHECK_TOTALLY_SAME_ACCOUNTS_OUTPUT_PATH.split('/')[1]
            if (!fs.existsSync('./' + process.env.WALLET_CHECK_TOTALLY_SAME_ACCOUNTS_OUTPUT_PATH.split('/')[1])) {
                fs.mkdirSync(outputFolder)
            }

            const outputStream = fs.createWriteStream(outputFilePath);
            const outputStream_notInDB_nameOnly = fs.createWriteStream(outputFilePath_notInDB_nameOnly);
            const outputStreamNameOnly = fs.createWriteStream(outputFilePath_nameOnly);
            const outputStream_set = fs.createWriteStream(outputFilePath_set);
            const outputStream_inDB = fs.createWriteStream(outputFilePath_inDB);
            const outputStream_inDB_nameOnly = fs.createWriteStream(outputFilePath_inDB_nameOnly);

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
                this.scanCounts++
                const lineArray = line.split('----')
                const obj = {};
                obj.reason = lineArray[0]
                obj.account = lineArray[1];
                const ridAndPayload = JSON.parse(lineArray[2]).value
                outputStream_notInDB_nameOnly.write(obj.account + '\n')

                if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                    console.log('完全一樣的帳號:', obj.account)
                    this.toallySameAccoutRidCounts++
                    this.totallySameAccount.add(obj.account);
                    this.allAccounts[obj.account].rid_preSet_noInsert.push(ridAndPayload)
                    this.allAccounts[obj.account].hasTotallySameAccount++
                    return;
                }

                if (!this.accountMapGroup[obj.account.toLowerCase().trim()]) {
                    this.group++
                    this.accountMapGroup[obj.account.toLowerCase().trim()] = this.group;
                }

                obj.group = this.accountMapGroup[obj.account.toLowerCase().trim()];
                obj.rid_preSet_noInsert = [ridAndPayload]
                obj.rid_preSet_Insert = []
                obj.rid_updateDate_noInsert = []
                obj.rid_updateDate_Insert = []
                obj.hasTotallySameAccount = 1
                //console.log(obj)
                this.allAccounts[obj.account] = obj;
            })

            rl.on('error', function (e) {
                console.log(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('close', async () => {
                console.log('--------開始處理已儲存DB的大小寫重複帳號資料------------')
                //取得提前已insert的accounts
                const allNoInsertAccounts = Object.keys(this.allAccounts)
                await this.conn();
                const [accountsInserted] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account in (?)', [allNoInsertAccounts]);
                this.caseSensitiveAccountsInDB += accountsInserted.length;
                console.log('已存在DB的大小寫重複帳號: 共',accountsInserted.length + ' 筆')
                accountsInserted.forEach(e => {
                    const obj = {};
                    obj.reason = '已存在DB的大小寫重複帳號'
                    obj.account = e.account;
                    const ridAndPayload = { rid: e.rid, v: JSON.parse(e.payload) };
                    outputStream_inDB.write(`[${obj.reason}]----${obj.account}----${JSON.stringify({ value: ridAndPayload })}'\n'`)
                    outputStream_inDB_nameOnly.write(obj.account + '\n')

                    if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                        this.toallySameAccoutRidCounts++
                        this.totallySameAccount.add(obj.account);
                        this.allAccounts[obj.account].rid_preSet_Insert.push(ridAndPayload)
                        this.allAccounts[obj.account].hasTotallySameAccount++
                        outputStreamNameOnly.write(obj.account + '\n')
                        return;
                    }

                    if (!this.accountMapGroup[obj.account.toLowerCase().trim()]) {
                        this.group++
                        this.accountMapGroup[obj.account.toLowerCase().trim()] = this.group;
                    }

                    obj.group = this.accountMapGroup[obj.account.toLowerCase().trim()];
                    obj.rid_preSet_noInsert = []
                    obj.rid_preSet_Insert = [ridAndPayload]
                    obj.rid_updateDate_noInsert = []
                    obj.rid_updateDate_Insert = []
                    obj.hasTotallySameAccount = 1

                    this.allAccounts[obj.account] = obj;
                })

                const hasTotallySameAccount = [...this.totallySameAccount]
                console.log(hasTotallySameAccount)
                hasTotallySameAccount.forEach((e,i) => {
                    console.log(`${i+1}.玩家帳號:${e} 有${this.allAccounts[e].hasTotallySameAccount}個rid有完全一樣的帳號 \n`)
                    // let text = `帳號:${e} 有${this.allAccounts[e].hasTotallySameAccount}個rid有完全一樣的帳號 \n
                    // 提前_未insert DB [${this.allAccounts[e].rid_preSet_noInsert.length}個] : ${JSON.stringify(this.allAccounts[e].rid_preSet_noInsert)}\n
                    // 提前_insert DB  [${this.allAccounts[e].rid_preSet_Insert.length}個]: ${JSON.stringify(this.allAccounts[e].rid_preSet_Insert)}\n                `
                    // outputStream.write(text + '\n');

                    let text_B = `${i + 1}.玩家帳號:${e} 有${this.allAccounts[e].hasTotallySameAccount}個rid有完全一樣的帳號 \n   未INSERT DB的RID[共${this.allAccounts[e].rid_preSet_noInsert.length}筆]\n : ${JSON.stringify(this.allAccounts[e].rid_preSet_noInsert)}\n     已INSERT DB的RID[共${this.allAccounts[e].rid_preSet_Insert.length}筆]\n : ${JSON.stringify(this.allAccounts[e].rid_preSet_Insert)}\n`
                    outputStream.write(text_B + '\n');
                })

                let resultText = `大小寫重複玩家rid共有${this.scanCounts}筆，以上共有${hasTotallySameAccount.length}組玩家帳號有完全一樣的帳號，影響rid共有${this.toallySameAccoutRidCounts}筆`
                if (hasTotallySameAccount.length > 0) {
                    resultText += `，請至${outputFilePath}查看其details，確認要用哪一個rid作為正確的insert wallet 資訊! \n`
                }
                console.log(resultText)
                outputStream.write(resultText + '\n');
                outputStream.end();
                outputStreamNameOnly.end();
                outputStream_inDB.end();
                outputStream_inDB_nameOnly.end();
                outputStream_set.write(JSON.stringify(this.allAccounts));
                outputStream_set.end()
                console.log('文件处理完成，结果写入', outputFilePath);
                console.log('文件处理完成，结果写入(只列出有完全一樣的玩家帳號名)', outputFilePath_nameOnly);
                console.timeEnd("EXEC")
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check;