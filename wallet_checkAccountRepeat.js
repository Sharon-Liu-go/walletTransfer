const dotenv = require('dotenv');
dotenv.config();

// const walletTransfer_preset_check = require('./class/walletTransfer_preset_check');
// const checkRepeatedAccounts = new walletTransfer_preset_check;
// checkRepeatedAccounts.exec();


const walletTransfer_check = require('./class/walletTransfer_check');
const checkRepeatedAccounts = new walletTransfer_check;
checkRepeatedAccounts.exec();
