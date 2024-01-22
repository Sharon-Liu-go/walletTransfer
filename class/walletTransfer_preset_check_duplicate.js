
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
        this.totallySameAccount = 0 //相同帳號但pid不同
        this.similarAccount = 0 //大小寫帳號
        this.weildAccount = 0 //在code中不同，但DB視為相同。
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
                console.log('輸出檔案已存在，進行刪除');
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
            console.log('Mysql連線Config: ', config.mysql)
            console.log('--------------------')
            // 处理每一行的逻辑
            rl.on('line', async (line) => {
                const data = JSON.parse(line)
                console.log(`此批有${data.length}`)
                const accountAndRidMapping = {}
                const lowercaseAccounts = []
                const accounts = data.map(e => {
                    accountAndRidMapping[e[0]] = e
                    lowercaseAccounts.push(e[0].toLowerCase().trim())
                    return e[0]
                })

                await this.conn();
                const [players] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account in (?)', [accounts]);

                console.log(`此批從DB撈出有${players.length}`)

                const toTallySameAccountInDB = [];
                const similarAccount = [];
                const weildAccountInDB = [];

                players.forEach(e => {
                    const index = accounts.indexOf(e.account);
                    if (index != -1 && e.rid == accountAndRidMapping[e.account][2]) { //account和pid都完全相同
                        delete accountAndRidMapping[e.account]
                    } else if (index != -1) { //account相同但pid不相同 => 若有,需特別列出，因要確認同個帳號應該要用哪個pid才正確
                        let obj = {}
                        obj[e.account] = JSON.stringify(e);
                        toTallySameAccountInDB.push(obj)
                        this.totallySameAccount = 0 //相同帳號但rid不同
                    } else if (lowercaseAccounts.includes(e.account.toLowerCase().trim())) { //account 與 DB的大小寫不同
                        similarAccount.push(accountAndRidMapping[e.account][0])
                        this.similarAccount++
                    } else {
                        weildAccountInDB.push(e.account);
                        this.weildAccount++ //在code中不同，但DB視為相同。
                    }
                })

                outputStream.write(JSON.stringify(accountAndRidMapping) + '\n');
                outputStream.write(`帳號完全一樣但rid不同有${this.totallySameAccount}筆: 請確認該帳號要以哪一個rid作為正確的insert wallet 資訊!\n` + JSON.stringify(toTallySameAccountInDB) + '\n');
                outputStream.write(`大小寫帳號有${this.similarAccount}筆:\n` + JSON.stringify(similarAccount) + '\n');
                outputStream.write(`詭異帳號有${this.weildAccount}筆\n`+ '在DB的帳號:' + JSON.stringify(weildAccountInDB) + '\n');
                outputStream.write(`此批Insert時造成duplicate的帳號有:`+ Object.keys(accountAndRidMapping).length + '筆' + `其中帳號完全一樣但rid不同有${this.totallySameAccount}筆、大小寫帳號有${this.similarAccount}、詭異帳號有${this.weildAccount}筆\n` + '--------------------------------\n');
                console.log(`檢查結果: 截至目前Insert時造成duplicate的帳號共有${this.totallySameAccount + this.similarAccount + this.weildAccount}個， 其中帳號完全一樣但rid不同有${this.totallySameAccount}筆、大小寫帳號有${this.similarAccount}、詭異帳號有${this.weildAccount}，詳細結果寫入`, outputFilePath);
            })

            rl.on('error', function (e) {
                console.log(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('close', async () => {
                console.log(`掃描warningStatus檔案完畢`);
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check_duplicate;