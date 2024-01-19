
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
        this.totallySameAccount = new Set() //不重複的相同大小帳號
        this.allAccounts = {};
        this.group = 0
        this.accountMapGroup = {};
        this.allNoInsertAccounts = []; //for select accounts from mysql -> 拿allAccounts key 就好
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
            const inputFilePath =  process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SOURCE_PATH ;
            const outputFilePath = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_OUTPUT_PATH;

            console.log('inputFilePath: ', inputFilePath)
            console.log('outputFilePath: ', outputFilePath)

            //建立output 的資料夾
            const outputFolder = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_OUTPUT_PATH.split('/')[1]
            if (!fs.existsSync('./' + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_OUTPUT_PATH.split('/')[1])) {
                fs.mkdirSync(outputFolder)
            }

            if (fs.existsSync(outputFilePath)) {
              // 刪除檔案
              fs.unlink(outputFilePath, (err) => {
                if (err) {
                  console.error(`刪除檔案時發生錯誤: ${err}`);
                } else {
                  console.log('檔案已成功刪除。');
                }
              });
            }

            const outputStream = fs.createWriteStream(outputFilePath);

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
                const lineArray = line.split('----')
                const obj = {};
                obj.reason = lineArray[0]
                obj.account = lineArray[1];
                const ridAndPayload = JSON.parse(lineArray[2]).value

                if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                    console.log('完全一樣的帳號:', obj.account)
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
                console.log('已存在DB的大小寫重複帳號: 共',accountsInserted.length + ' 筆')
                accountsInserted.forEach(e => {
                    const obj = {};
                    obj.reason = '已存在DB的大小寫重複帳號'
                    obj.account = e.account;
                    const ridAndPayload = {rid:e.rid, v: JSON.parse(e.payload)};

                    if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                        this.totallySameAccount.add(obj.account);
                        this.allAccounts[obj.account].rid_preSet_Insert.push(ridAndPayload)
                        this.allAccounts[obj.account].hasTotallySameAccount++
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
                hasTotallySameAccount.forEach(e => {
                    console.log(`帳號:${e} 有${this.allAccounts[e].hasTotallySameAccount}個rid有完全一樣的帳號 \n`)
                    let text = `帳號:${e} 有${this.allAccounts[e].hasTotallySameAccount}個rid有完全一樣的帳號 \n
                    提前_未insert DB : ${JSON.stringify(this.allAccounts[e].rid_preSet_noInsert)}\n
                    提前_insert DB : ${JSON.stringify(this.allAccounts[e].rid_preSet_Insert)}\n                `
                    outputStream.write(text + '\n');
                })

                let resultText = `以上共有${hasTotallySameAccount.length}組玩家帳號有完全一樣的帳號`
                if (hasTotallySameAccount.length > 0) {
                    resultText += `，請至${outputFilePath}查看其details，確認要用哪一個rid作為正確的insert wallet 資訊! \n`
                }
                console.log(resultText)
                outputStream.write(resultText + '\n');
                outputStream.end();
                console.log('文件处理完成，结果写入', outputFilePath);
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check;