
const mysql = require('mysql2/promise');
const util = require('../util');

const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};


const fs = require('fs');

class walletTransfer_preset_check {
    constructor(dirname) {
        this.dirname = dirname;
        this.mysqlConn;
        // for report
        this.totallySameAccount = new Set() //不重複的相同大小帳號
        this.allAccounts = {};
        this.group = 0
        this.accountMapGroup = {};
        this.allNoInsertAccounts = []; //for select accounts from mysql -> 拿allAccounts key 就好
    }

    async conn() {
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
            //const inputFilePath =  this.dirname   +  process.env.WALLET_CHECK_REPEATED_ACCOUNTS_SOURCE_PATH ;
            //const outputFilePath =  this.dirname    + process.env.WALLET_CHECK_REPEATED_ACCOUNTS_OUTPUT_PATH;



            // console.log('inputFilePath', inputFilePath)
            // console.log('outputFilePath', outputFilePath)

            //建立output 的資料夾
            // const outputFolder = process.env.WALLET_CHECK_REPEATED_ACCOUNTS_OUTPUT_PATH.split('/')[0]
            // fs.mkdirSync(outputFolder)

            // 创建逐行读取的接口
            const rl = fs.createReadStream('./export/batchHgetAllValue_account_repeat.txt');
            // 設置編碼爲 utf8。
            rl.setEncoding("UTF8");
            console.log('OK')
            // 处理每一行的逻辑
            rl.on('data', (line) => {
                console.log(line)
                const lineArray = line.split('----')
                console.log(lineArray)
                const obj = {};
                obj.reason = lineArray[0]
                obj.account = lineArray[1];
                const ridAndPayload = JSON.parse(lineArray[2]).value
                console.log("content", ridAndPayload)

                if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                    this.totallySameAccount.add(obj.account);
                    obj.rid_preSet_noInsert.push(ridAndPayload)
                    obj.hasTotallySameAccount++
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
                console.log(obj)
                this.allAccounts[obj.account] = obj;
                console.log(this.allAccounts)
            })

            rl.on('error', function (e) {
                console.log(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('end', async () => {
                //取得提前已insert的accounts
                const allNoInsertAccounts = Object.keys(this.allAccounts)
                await this.conn();
                const accountsInserted = await this.mysqlConn.query('SELECT * wallet.players where name in (?)', allNoInsertAccounts);

                accountsInserted.forEach(e => {
                    const obj = {};
                    obj.reason = lineArray[0]
                    obj.account = lineArray[1];
                    const ridAndPayload = JSON.parse(lineArray[2]).value;

                    if (this.allAccounts[obj.account]) { //若已存在代表完全一樣的account
                        this.totallySameAccount.add(obj.account);
                        obj.rid_preSet_Insert.push(ridAndPayload)
                        obj.hasTotallySameAccount++
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

                const outputStream = fs.createWriteStream(outputFilePath);

                const hasTotallySameAccount = [...this.totallySameAccount]
                hasTotallySameAccount.forEach(e => {
                    console.log(`帳號:${e.account} 有${e.hasTotallySameAccount}個完全一樣的帳號 \n`)
                    let text = `帳號:${e.account} 有${e.hasTotallySameAccount}個完全一樣的帳號 \n
                    提前_未insert DB : ${e.rid_preSet_Insert}\n
                    提前_insert DB : ${e.rid_preSet_Insert}\n                `
                    outputStream.write(text + '\n');
                })
                console.log(`以上共有${hasTotallySameAccount.length}組玩家帳號有完全一樣的帳號，請至${outputFilePath}查看其details，確認要用哪一個rid作為正確的insert wallet 資訊! \n`)
                outputStream.end();
                console.log('文件处理完成，结果写入', outputFilePath);
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check;