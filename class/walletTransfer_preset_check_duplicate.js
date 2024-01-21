
const mysql = require('mysql2/promise');
const util = require('../util');

const config = {
    redis: require('../config/redis'),
    mysql: require('../config/mysql')
};


const fs = require('fs');
const readline = require('readline');

class walletTransfer_preset_check_duplicate {
    constructor() {
        this.mysqlConn;
        // for report
        this.weildAccount = new Set() //不重複的相同大小帳號
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
            const inputFilePath = process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_SOURCE_PATH;
            const outputFilePath = process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_OUTPUT_PATH;

            console.log('inputFilePath: ', inputFilePath)
            console.log('outputFilePath: ', outputFilePath)

            //建立output 的資料夾
            const outputFolder = process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_OUTPUT_PATH.split('/')[1]
            if (!fs.existsSync('./' + process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_OUTPUT_PATH.split('/')[1])) {
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
            const fileStream = fs.createReadStream(inputFilePath);

            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity,
            })
            console.log('--------------------')
            // 处理每一行的逻辑
            rl.on('line', async (line) => {
                const accountAndRidMapping = {}
                const accounts = line.map(e => {
                    accountAndRidMapping[e[0]] = e
                    return e[0]
                })
                await this.conn();
                const [players] = await this.mysqlConn.query('SELECT name FROM wallet.player where name in (?)', [accounts]);

                players.forEach(e => {
                    const index = accounts.indexOf(e.name);
                    if (index != -1) {
                        delete accountAndRidMapping[e.name]
                    }
                })

                outputStream.write(JSON.stringify(accountAndRidMapping) + '\n');
                this.weildAccout += Object.keys(accountAndRidMapping).length
            })

            rl.on('error', function (e) {
                console.log(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('close', async () => {
                outputStream.write(this.weildAccout + '\n');
                outputStream.end();
                console.log(`檢查warningStatus檔案完畢，Insert時造成duplicate的帳號共有${this.weildAccout.length}個， 成，${this.weildAccount}詳細結果寫入`, outputFilePath);
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check_duplicate;