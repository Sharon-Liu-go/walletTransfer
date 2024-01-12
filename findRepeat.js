const fs = require('fs');
const readline = require('readline');

// 讀取檔案的路徑
const filePath = './export/warningStatus.json'; // 請換成你的檔案路徑

// 創建讀取檔案的介面
const fileStream = fs.createReadStream(filePath);
const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity // 當輸入有換行時，使用 Infinity 代表不切割行
});

const array = require('./export/uniqueAccount.json')
const uniqure = [];
array.forEach(e => {

  if (uniqure.includes(e)) {
    console.log(e)
    return;
  }
  console.log(index + 1 )
  uniqure.push(e)
  
});



// array.forEach(e => {
//     try {
//     // 寫入檔案
//   fs.appendFile('./line.txt', e[0]+ '\n', (err) => {
//   if (err) {
//     console.error('寫入檔案時發生錯誤：', err);
//     return;
//   }
//   console.log('成功寫入檔案！');
// });
//   } catch (err) {
//     console.log(err)
//   }
// })



// // 逐行讀取檔案
// rl.on('line', (line) => {
//   // 處理每一行的資料


  
// });

// // 當檔案讀取結束時觸發
// rl.on('close', () => {
//   console.log(array.length)
//   console.log('檔案讀取完畢！');
// });