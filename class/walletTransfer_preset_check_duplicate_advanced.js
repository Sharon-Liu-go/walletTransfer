
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
        this.readTimes = 0 //讀取次數
        this.totallySameAccount = 0 //相同帳號但pid不同
        this.similarAccount = 0 //大小寫帳號
        this.weildAccount = 0 //在code中不同，但DB視為相同。
        this.shouldInDBbutNot = 0 //奇怪，DB沒有該類似帳號，但該帳號卻沒有儲存在DB裡    
        this.outputStream_repeat;
        this.outputStream_weild;
        this.outputStream_shouldInDBbutNot;
    }

    async conn() {
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

    async examData(data,batchNo) {
        console.log(`此第${batchNo}批有${data.length}`);  
        let totallySameAccount = 0 //相同帳號但pid不同
        let similarAccount = 0 //大小寫帳號
        let weildAccount = 0 //在code中不同，但DB視為相同。
        let shouldInDBbutNot = 0 //奇怪，DB沒有該類似帳號，但該帳號卻沒有儲存在DB裡 
        await Promise.allSettled(data.map(async e => {
            const [players] = await this.mysqlConn.query('SELECT * FROM wallet.player_info where account =?', [e[0]]);
            if (players.length > 0) {
                if (e[0] === players[0].account) {
                    if (players[0].rid === e[2]) {  //帳號和rid完全一樣。
                        return;
                    }
                    this.totallySameAccount++  //帳號一樣，但rid不一樣，需特別列出，因要確認同個帳號應該要用哪個pid才正確。
                    totallySameAccount++
                    this.outputStream_repeat.write(`[duplicate大小寫帳號:帳號完全一樣但rid不一樣]----${e[0]}----${JSON.stringify({ value: { rid: e[2], v: e[3] } })}\n`);
                    return
                }
                if (e[0].toLowerCase().trim() === players[0].account.toLowerCase().trim()) { //大小寫帳號
                    this.similarAccount++
                    similarAccount++
                    this.outputStream_repeat.write(`[duplicate大小寫帳號]----${e[0]}----${JSON.stringify({ value: { rid: e[2], v: e[3] } })}\n`);
                    return
                }
                this.weildAccount++ //除了以上，js視為不同，但DB卻視為相同
                weildAccount++
                this.outputStream_weild.write(`[詭異帳號]----${e[0]}----${JSON.stringify({ value: { rid: e[2], v: e[3] } })}\n`);
                return
            }
            this.shouldInDBbutNot++
            shouldInDBbutNot++
            this.outputStream_shouldInDBbutNot.write(`[DB無該類似帳號但卻沒存在DB]----${e[0]}----${JSON.stringify({ value: { rid: e[2], v: e[3] } })}\n`);
            return
            
        }))
        console.log(`此第${[batchNo]}批造成insert duplicate的帳號:${totallySameAccount + similarAccount + weildAccount + shouldInDBbutNot} : 帳號完全一樣的大小寫:${totallySameAccount}筆、大小寫帳號:${similarAccount}筆、詭異帳號:${weildAccount}筆、應存在DB但沒有的${shouldInDBbutNot}筆`)
        return;
    }
    /**
     * TODO: 共用
     */
    async valid() { }

    async exec() {
        try {
            util.log('exec')
            console.time('EXEC')
            const inputFilePath = process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_SOURCE_PATH;
            const outputFilePath_repeat = process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_OUTPUT_PATH;
            const outputFilePath_weild = process.env.WALLET_CHECK_WEILD_ACCOUNTS_OUTPUT_PATH;
            const outputFilePath_shouldInDBbutNot = process.env.WALLET_CHECK_SHOULD_EXIST_ACCOUNTS_OUTPUT_PATH;

            console.log('inputFilePath: ', inputFilePath)
            console.log('outputFilePath_repeat: ', outputFilePath_repeat)
            console.log('outputFilePath_weild: ', outputFilePath_weild)
            console.log('outputFilePath_shouldInDBbutNot: ', outputFilePath_shouldInDBbutNot)

            //確認input檔案是否存在
            if (!fs.existsSync(process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_SOURCE_PATH)) {
                console.log(`檢驗資料檔案不存在:${process.env.WALLET_CHECK_DUPLICATE_ACCOUNTS_SOURCE_PATH}，請確認是沒有該檔案錯誤，還是.env提供的位置有誤`)
            }

            //建立output 的資料夾
            const output = process.env.WALLET_CHECK_WEILD_ACCOUNTS_OUTPUT_PATH.split('/')
            const outputFolderPath = './' + output[1] + '/' + output[2];
            console.log('outputFolderPath:',outputFolderPath)
            if (!fs.existsSync(outputFolderPath)) {
                fs.mkdirSync(outputFolderPath)
            }
            await this.removeFileIfExist(outputFilePath_repeat)
            await this.removeFileIfExist(outputFilePath_weild)
            await this.removeFileIfExist(outputFilePath_shouldInDBbutNot)
           
            this.outputStream_repeat = fs.createWriteStream(outputFilePath_repeat);
            this.outputStream_weild = fs.createWriteStream(outputFilePath_weild);
            this.outputStream_shouldInDBbutNot = fs.createWriteStream(outputFilePath_shouldInDBbutNot);

            // 创建逐行读取的接口
            const fileStream = fs.createReadStream(inputFilePath);

            const rl = readline.createInterface({
                input: fileStream,
                crlfDelay: Infinity,
            })
            console.log('Mysql連線Config: ', config.mysql)
            console.log('--------------------')
            
            await this.conn();
            const promises = [];

            // 处理每一行的逻辑
            rl.on('line', async (line) => {
                this.readTimes++
                const data = JSON.parse(line); 
                let batchNo = this.readTimes;
                promises.push(this.examData(data,batchNo))
            })
        
            rl.on('error', function (e) {
                console.error(e)
            });

            // 在文件读取结束时关闭可写流
            rl.on('close', async () => {
                console.log(`掃描warningStatus檔案完畢,讀取次數:${this.readTimes}`);
                Promise.all(promises).then(() => {
                    console.log(`掃描warningStatus檔案總次數:${this.readTimes}`);
                    console.log('所有非同步操作已完成。');
                    console.log(`累積造成insert duplicate的帳號:${this.totallySameAccount + this.similarAccount + this.weildAccount + this.shouldInDBbutNot } : 帳號完全一樣的大小寫:${this.totallySameAccount}筆、大小寫帳號:${this.similarAccount}筆、詭異帳號:${this.weildAccount}筆、應存在DB但沒有的${this.shouldInDBbutNot}筆`)
                    console.timeEnd('EXEC')
                });            
            });

        } catch (err) {
            console.error(err)
        }

    }
}

module.exports = walletTransfer_preset_check_duplicate;