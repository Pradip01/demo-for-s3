/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var EventEmitter = require('events').EventEmitter,
	fork 		 = require('child_process').fork,
	exec 		 = require('child_process').exec,
	express 	 = require('express'),
	_ 			 = require('lodash'),
	proto 		 = require('./application'),
	config 		 = require('../utils/config');

var req = express.request,
	res = express.response;

var contentstack = module.exports = ContentstackApplication;

/**
 * Override contentstack() and provide custom app instance
 */
function ContentstackApplication() {
	var app = function (req, res, next) {
		app.handle(req, res, next);
	};

	// if(!config.get('mode.threading')) {
		app = _.merge(app, EventEmitter.prototype);
		app = _.merge(app, proto);

		app.request = {__proto__: req, app: app};
		app.response = {__proto__: res, app: app};
		app.fuse();

		if(config.get('forkSync')) {
			var child = fork('./node_modules/contentstack-express/lib/sync/forkSync');
			console.log('Sync process started @', child.pid);
		} else {
			require('../sync')();
		}
	// } else {
	// 	var child = exec('node -e "require(\'./node_modules/contentstack-express/lib/sync/forkSync\')"');
	// 	child.stdout.on('data', function(data) {
	// 	    console.log('>>: ' + data);
	// 	});
	// 	child.stderr.on('data', function(data) {
	// 	    console.log(data + ' :!!');
	// 	});
	// 	child.on('close', function(code) {
	// 	    console.log('Process exitting: ' + code);
	// 	    process.exit(code);
	// 	});
	// 	// exec('node -e "require(\'./node_modules/contentstack-express/lib/sync/forkSync\')"', function (error, stdout, stderr) {
	// 	// 	if (error) {
	// 	// 		console.error(`exec error: ${error}`);
	// 	// 		process.exit(1);
	// 	// 	}
	// 	// });
	// }

	return app;
}

/**
 * Expose the prototypes.
 */
contentstack.application = proto;
contentstack.request = req;
contentstack.response = res;

/**
 * Expose router
 */
contentstack.Router = require('./router');
contentstack.Route = express.Route;

/**
 * Expose middleware
 */
contentstack.query = express.query;
contentstack.static = express.static;

