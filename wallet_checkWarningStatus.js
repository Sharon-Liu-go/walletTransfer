const dotenv = require('dotenv');
dotenv.config();

const walletTransfer_preset_check_duplicate = require('./class/walletTransfer_preset_check_duplicate');
const checkDuplicateAccounts = new walletTransfer_preset_check_duplicate;
checkDuplicateAccounts.exec(); 
