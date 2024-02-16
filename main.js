const dotenv = require('dotenv');
const dayjs = require('dayjs')
dotenv.config();

const KeyCounter = require('./class/KeyCounter');
const Validator = require('./class/__Validator');
const WalletValidator = require('./class/WalletValidator');
const Currency = require('./class/Currency');
//const findLackPlayers = require('./class/findLackPlayers');
const walletTransfer_preset = require('./class/walletTransfer_preset');
const walletTransfer_update = require('./class/walletTransfer_update');
const walletTransfer_check = require('./class/walletTransfer_check');
const walletTransfer_preset_check_duplicate = require('./class/walletTransfer_preset_check_duplicate');

const util = require('./util');

const TIMER = {
    PROCCESS: 'Total proccess cost',
    GET_ACCOUNT: 'Get Account',
    GET_RID: 'Get rid',
    GET_PLAYER: 'Get Player'
}

const pattern = process.env.COMMAND_PATTERN || '';

console.log('當前檔案的位置:', __dirname);

const menu = [
	{
		msg: 'Key Count',
		entity: {
			exec: async () => {
				const [flow] = await util.options(target);
				await util.sleep(2000);
				
				console.time(TIMER.PROCCESS);
				await flow.exec(); 
				console.timeEnd(TIMER.PROCCESS);
			}
		},
		reply: 'Key Count'
	},
	{
		msg: 'Account Valid',
		entity: {
			exec: async () => {
				const flow = new WalletValidator('Account:*')
				await util.sleep(2000);
				
				console.time(TIMER.PROCCESS);
				await flow.exec(); 
				console.timeEnd(TIMER.PROCCESS);
			}
		},
		reply: 'Chosing Account Valid'
	},
	{
		msg: 'Account Valid(Within Mysql)',
		entity: {
			exec: async () => {
				const flow = new WalletValidator('Account:*', true)
				await util.sleep(2000);
				
				console.time(TIMER.PROCCESS);
				await flow.exec(); 
				console.timeEnd(TIMER.PROCCESS);
			}
		},
		reply: ''
	},
	{
		msg: 'Currency',
		entity: {
			exec: async () => {
				console.time(TIMER.PROCCESS);
				const currency = new Currency();
				
				await currency.update(); 
				console.log('cny', await currency.exchange('cny'))
				console.log(2, await currency.exchange(2))

				console.timeEnd(TIMER.PROCCESS);
			}},
		reply: ''
	},
	{
		msg: 'Old Version Validator',
		entity: new Validator('Account:*', true),
		reply: ''
	},
	// {
	// 	msg: 'Find Out Lack Players',
	// 	entity: new findLackPlayers('Player:*', true),
	// 	reply: ''
	// },
	{
		msg: 'wallet transfer preset',
		entity: new walletTransfer_preset('Player:*', true),
		reply: ''
	},
	{
		msg: 'wallet transfer update',
		entity: new walletTransfer_update('Player:*', true),
		reply: ''
	},
	{
		msg: 'wallet transfer warningStatus analysis',
		entity: new walletTransfer_preset_check_duplicate(),
		reply: ''
	},
	{
		msg: 'wallet transfer check',
		entity: new walletTransfer_check(),
		reply: ''
	},
]
const target = [
	{
		msg: 'Account:*',
		entity: new KeyCounter('Account:*'),
		reply: ''
	},
	{
		msg: 'Player:*',
		entity: new KeyCounter('Player:*'),
		reply: ''
	},
	{
		msg: 'Get Pattern from .env',
		entity: new KeyCounter(pattern),
		reply: `Pattern is [${pattern}]`
	},
	{
		msg: 'Get Pattern from input',
		entity: {
			exec: async () => {
				const [pattern] = await util.readline([
					{ msg: 'Pattern, ex: Account:*' }
				]);

				const flow = new KeyCounter(pattern);
				await flow.exec(); 
			}
		},
		reply: ''
	},
]

const main = async () => {
	try {
		const [flow] = await util.options(menu);
		await util.sleep(500);
		await flow.exec();
	} catch (e) {
		if (e.code) {
			console.log(e);
		} else {
			console.log(e.stack);
		}
	}
}

main().then(process.exit)
