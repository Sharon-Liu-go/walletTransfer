const dotenv = require('dotenv');
dotenv.config();

const walletTransfer_preset_check_duplicate = require('./class/walletTransfer_preset_check');
const checkRepeatedAccounts = new walletTransfer_preset_check;
await checkRepeatedAccounts.exec();

const walletTransfer_preset_check_duplicate = require('./class/walletTransfer_preset_check_duplicate');
const checkDuplicateAccounts = new walletTransfer_preset_check_duplicate;
await checkDuplicateAccounts.exec();