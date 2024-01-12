const fs = require('fs');
const path = require('path');
const readline = require('readline');

const utils = {};
const useConsoleLog = true;

utils.log = function (...args) {
	if (useConsoleLog) {
		console.log(args.join('\n'));
	}
}

utils.error = function (...args) {
	if (useConsoleLog) {
		console.error(args.join('\n'));
	}
}

//去空格
utils.Trim = function (v) {
	return v.replace(/^\s+|\s+$/g, "");
}
utils.formatMoney = function (value, point) {
	var sign = value < 0 ? '-' : '';
	if (point == undefined) {
		return sign + formatNumber(Math.abs(value), '#,##0.00');
	} else {
		return sign + formatNumber(Math.abs(value), '#,##0');
	}
};

/**  
* 格式化数字显示方式   
* 用法  
* formatNumber(12345.999,'#,##0.00');  
* formatNumber(12345.999,'#,##0.##');  
* formatNumber(123,'000000');
*/
var formatNumber = function (v, pattern) {
	if (v == null)
		return v;
	var strarr = v ? v.toString().split('.') : ['0'];
	var fmtarr = pattern ? pattern.split('.') : [''];
	var retstr = '';
	// 整数部分   
	var str = strarr[0];
	var fmt = fmtarr[0];
	var i = str.length - 1;
	var comma = false;
	for (var f = fmt.length - 1; f >= 0; f--) {
		switch (fmt.substr(f, 1)) {
			case '#':
				if (i >= 0) retstr = str.substr(i--, 1) + retstr;
				break;
			case '0':
				if (i >= 0) retstr = str.substr(i--, 1) + retstr;
				else retstr = '0' + retstr;
				break;
			case ',':
				comma = true;
				retstr = ',' + retstr;
				break;
		}
	}
	if (i >= 0) {
		if (comma) {
			var l = str.length;
			for (; i >= 0; i--) {
				retstr = str.substr(i, 1) + retstr;
				if (i > 0 && ((l - i) % 3) == 0) retstr = ',' + retstr;
			}
		}
		else retstr = str.substr(0, i + 1) + retstr;
	}
	retstr = retstr + '.';
	// 处理小数部分   
	str = strarr.length > 1 ? strarr[1] : '';
	fmt = fmtarr.length > 1 ? fmtarr[1] : '';
	i = 0;
	for (var f = 0; f < fmt.length; f++) {
		switch (fmt.substr(f, 1)) {
			case '#':
				if (i < str.length) retstr += str.substr(i++, 1);
				break;
			case '0':
				if (i < str.length) retstr += str.substr(i++, 1);
				else retstr += '0';
				break;
		}
	}
	return retstr.replace(/^,+/, '').replace(/\.$/, '');
};

utils.wrap = async (promiseFn, defaultResult = []) => {
	let error,
		result = defaultResult;
	try {
		result = await promiseFn;
	} catch (err) {
		error = err;
		console.error(err);
	}
	return { result, error };
};


utils.sleep = (ms, msg) => {
    if (msg) this.log(msg)
    return new Promise(resolve => setTimeout(resolve, ms));
};

utils.array2Map = (arr, key) => {
	const m = new Map();

	for (const row of arr) {
		m.set(row[key], row);
	}

	return m;
}

utils.save = (filePath, data, recover) => {
	if (recover) {
		return;
	}
    if (!fs.existsSync(path.resolve(__dirname, filePath))) {//检查配置是否存在
        fs.writeFileSync(path.resolve(__dirname, filePath), '');
    }
    fs.appendFileSync(path.resolve(__dirname, filePath), data + '\n')
}

utils.options = (opts) => {
	const rl = readline.createInterface({
		input: process.stdin, // 指定输入流为控制台输入
		output: process.stdout // 指定输出流为控制台输出
	});

	return new Promise((next) => {
		let o = '';

		for (const index in opts) {
			o += `  ${Number(index) + 1}. ${opts[index].msg}\n`
		}

		const q = '請選擇進行的操作,\n' + o + '  0. exit\n  > ';

		rl.question(q, (answer) => {
			rl.close();

			if (!isNaN(answer) && opts[Number(answer) - 1]) {
				const { entity, reply } = opts[Number(answer) - 1];
				if (reply) {
					utils.log(reply);
				}
				return next([entity, answer]);
			}

			return next([{ exec: () => {} }, answer]);
		});
	})
}

/**
 * 
 * @param {Array} opts 
 * @param {Object} opts[] 
 * @param {String} opts[].msg 輸入參數前的提示
 * @param {String} opts[].reply 輸入參數後的打印
 * @returns {Array} 依序返回終端機input
 */
utils.readline = (opts) => {
	const rl = readline.createInterface({
		input: process.stdin, // 指定输入流为控制台输入
		output: process.stdout // 指定输出流为控制台输出
	});

	const [opt] = opts;

	return new Promise((next) => {
		const q = `請輸入指定參數(${opt.msg}),\n  > `;

		rl.question(q, (answer) => {
			rl.close();
			
			if (opt.reply) {
				utils.log(opt.reply);
			}

			return next([answer]);
		});
	})
}

module.exports = utils;