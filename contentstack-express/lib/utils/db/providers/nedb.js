/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var datastore       = require('nedb'),
    path            = require('path'),
    fs              = require('graceful-fs'),
    events          = require('events').EventEmitter,
    util            = require('util'),
    _               = require('lodash'),
    async           = require('async'),
    config          = require('../../config'),
    helper          = require('../helper'),
    InMemory        = require('../inmemory'),
    fileStorage     = require('./FileSystem'),
    languages       = config.get('languages'),
    assetDwldFlag   = config.get('assets.download'),
    // Common keys
    assetRoute      = '_assets',
    entryRoute      = '_routes',
    schemaRoute     = '_content_types',
    del_keys      = ['created_by', 'updated_by', '_data', 'include_references', '_remove', 'locale'],
    // These forms are loaded as part of Inmemory, thus skipping here
    skipForms       = [assetRoute, entryRoute, schemaRoute],
    cachedStructures = [assetRoute, schemaRoute, entryRoute];

var nedbStorage = function () {
    // Inherit methods from EventEmitter
    events.call(this);
    // Remove memory-leak warning about max listeners
    this.setMaxListeners(0);
    // Keep track of spawned child processes
    this.childProcesses = [];
    this.db = {};
    var self = this,
        databases = {};
    for (var l = 0, lTotal = languages.length; l < lTotal; l++) {
        databases[languages[l]['code']] = (function (language) {
            return function (cb) {
                self.db[language.code] = new datastore({inMemoryOnly: true});
                var model = language.contentPath;
                if (fs.existsSync(model)) {
                    fs.readdir(model, function (err, files) {
                        if (err) {
                            return cb(err, null);
                        } else {
                            var loadDatabase = [];
                            for (var i = 0, total = files.length; i < total; i++) {
                                var fileName = files[i].replace('.json', '');
                                if (skipForms.indexOf(fileName) === -1) {
                                    loadDatabase.push(function (i, filePath) {
                                        return function (_cb) {
                                            fs.readFile(path.join(model, filePath), 'utf-8', function (err, data) {
                                                if (err) return _cb(err);
                                                data = JSON.parse(data);
                                                self.db[language.code].insert(data, _cb);
                                            });
                                        }
                                    }(i, files[i]));
                                }
                            }
                            async.parallel(loadDatabase, function (err, res) {
                                if (err) {
                                    return cb(err, res);
                                } else {
                                    self.db[language.code].ensureIndex({
                                        fieldName: '_uid',
                                        unique: true,
                                        sparse: true
                                    }, cb);
                                }
                            });
                        }
                    });
                } else {
                    return cb(null, {});
                }
            }
        }(languages[l]));
    }

    async.parallel(databases, function (err, res) {
        if (err) {
            console.error('Error loading nedb:' + err.message);
            process.exit(0);
        }
    });
};

// Extend from base provider
util.inherits(nedbStorage, events);

// include references
nedbStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
  var self = this,
      calls = [];
  if (_.isEmpty(references)) references = {}
  var _includeReferences = function (data) {
    for (var _key in data) {
      if (data.uid) parentID = data.uid
      if (typeof data[_key] == "object") {
        if (data[_key] && data[_key]["_content_type_id"]) {
          calls.push(function (_key, data) {
            return (function (_callback) {
                var _uid = (data[_key]["_content_type_id"] === assetRoute && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                var query = {
                        "_content_type_uid": data[_key]["_content_type_id"],
                        "_uid": _uid,
                        "locale": _locale
                    },
                    _calls = [];

                if (query._content_type_uid !== assetRoute) {
                    query["_uid"]["$in"] = _.filter(query["_uid"]["$in"], function (uid) {
                        var flag = helper.checkCyclic(uid, references)
                        return !flag
                    });
                }
                _calls.push(function (field, query) {
                    return (function (cb) {
                        self.find(query, {}, function (_err, _data) {
                            if (!_err || (_err.code && _err.code === 194)) {
                                if (_data && (_data.entries || _data.assets)) {
                                    // to remove the wrapper from the result set
                                    _data = _data.entries || _data.assets;

                                    var __data = [];
                                    if (query._uid && query._uid.$in) {
                                        for (var a = 0, _a = query._uid.$in.length; a < _a; a++) {
                                            var _d = _.find(_data, {uid: query._uid.$in[a]});
                                            if (_d) __data.push(_d);
                                        }
                                        data[field] = __data;
                                    } else {
                                        data[field] = (_data && _data.length) ? _data[0] : {};
                                    }
                                } else {
                                    data[field] = [];
                                }
                                return setImmediate(function () {
                                    return cb(null, data)
                                });
                            } else {
                                return setImmediate(function () {
                                    return cb(_err, null);
                                });
                            }

                        }, _.cloneDeep(references), parentID);
                    });
                }(_key, query));
                async.series(_calls, function (__err, __data) {
                    return setImmediate(function () {
                        return _callback(__err, __data);
                    });
                });
            });
          }(_key, data));
        } else {
          _includeReferences(data[_key]);
        }
      }
    }
  };

  var recursive = function (data, callback) {
    _includeReferences(data);
    if (calls.length) {
      async.series(calls, function (e, d) {
        if (e) throw e;
        calls = [];
        return setImmediate(function () {
          return recursive(data, callback);
        });
      });
    } else {
      return callback(null, data);
    }
  };

  try {
    recursive(data, callback);
  } catch (e) {
    callback(e, null);
  }
};


/**
 * Find object based on query
 * @param  {Object}   query     : Query options
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */

nedbStorage.prototype.findOne = function (query, callback) {
    try {
        var _query = _.cloneDeep(query),
            self = this;

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && _.isFunction(callback)) {
            var content_type = _query._content_type_uid,
                locale = _query.locale,
                remove = _query._remove || false,
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true: false;

            // Delete unwanted keys
            // TODO: use _.omit instead
            _query = helper.filterQuery(_query);

            // For '_content_type_uid', '_assets' & '_routes' fetch data off InMemory
            if(cachedStructures.indexOf(content_type) !== -1) {
                var results = InMemory.get(locale, content_type, _query),
                    data = (results && results.length) ? results[0]: [];
                data = (remove) ? data: ((content_type === assetRoute) ? {asset: data}: {entry: data});
                return callback(null, data);
            } else {
                this.db[locale].findOne(_query).sort({"_data.published_at": -1}).exec(function (err, data) {
                    try {
                        if (err) throw err;
                        console.log(data);
                        if (data && data._data) {
                            // check if data exists then only search for more
                            var _data = (remove) ? data: data._data;
                            _data = (remove) ? _data: ((content_type === assetRoute) ? {asset: _data}: {entry: _data});
                            if (includeReference && content_type !== assetRoute)
                                self.includeReferences(_data, locale, undefined, undefined, callback);
                            else
                                return callback(null, _data);
                        } else {
                            // helper.generateCTNotFound(locale, _query._content_type_uid);
                            var _data = (remove) ? null: ((content_type === assetRoute) ? {asset: null}: {entry: null});
                            return callback(null, _data);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                });
            }
        } else {
            throw new Error('Query parameter should be of type `object` and should not be empty');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Find objects based on query & options
 * @param  {Object}   query     : Query options
 * @param  {Object}   options   : Options to be applied on the found objects
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */

/**
 * Querying doesn't seem to work on cachedStructures - since they're processed by InMemory
 */

nedbStorage.prototype.find = function (query, options, callback) {
    try {
        var self = this,
            _query = _.cloneDeep(query),
            references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3] : {},
            parentID = (_.isString(arguments[4])) ? arguments[4] : undefined;
        console.log('NEDB find()', query);
        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _.isPlainObject(options) && _query._content_type_uid && _query.locale && _.isFunction(callback)) {
            var locale = _query.locale,
                content_type = _query._content_type_uid,
                remove = _query._remove || false,
                typeKey = (content_type === assetRoute) ? 'assets': 'entries', // Add support for content type
                count = _query.include_count,
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true: false,
                calls = {}, queryObject;

            // Delete unwanted keys
            // TODO: use _.omit instead
            // _query = helper.filterQuery(_query);
            _query = _.omit(_query, del_keys);

            if (cachedStructures.indexOf(content_type) !== -1) {
                var results = InMemory.get(locale, content_type, _query),
                    data = (results && results.length) ? results: [];
                data = (remove) ? data: ((content_type === assetRoute) ? {assets: data}: {entries: data});
                return callback(null, data); 
            } else {
                queryObject = self.db[locale].find(_query).sort(options.sort || {"_data.published_at": -1});
                if (options.limit) queryObject.skip((options.skip || 0)).limit(options.limit);
                calls[typeKey] = function (_cb) {
                    queryObject.exec(_cb);
                };
                if (count) {
                    calls['count'] = function (_cb) {
                        self.db[locale].count(_query, _cb);
                    }
                }

                async.parallel(calls, function (error, result) {
                    try {
                        console.log('result', result);
                        if (error) throw error;
                        if (result && result[typeKey] && result[typeKey].length) {
                            var _data = _.cloneDeep(result);
                            _data[typeKey] = _.map(_data[typeKey], '_data');
                            console.log('find', _query, includeReference);
                            if (content_type !== assetRoute && includeReference) {
                                if (parentID) {
                                    var tmpResult = _data[typeKey];
                                    references[parentID] = references[parentID] || [];
                                    references[parentID] = _.uniq(references[parentID].concat(_.map(tmpResult, 'uid')));
                                }
                                self.includeReferences(_data, locale, references, parentID, callback);
                            } else {
                                return callback(null, _data);
                            }
                        } else {
                            // helper.generateCTNotFound(locale, query._content_type_uid);
                            return callback(null, result);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                });
            }
        } else {
            throw new Error('Query & options parameter should be of type `object` and `query` object should not be empty!');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// TODO: Short circuit find() to get only count information? Need clarification
/**
 * Find the count of the Content Type passed
 * @param  {Object}   query     : Query object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}           : Function to be called once this method is executed
 */

nedbStorage.prototype.count = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(query) && !_.isEmpty(query)) {
            var locale = query.locale;
            // to remove the unwanted keys from query and create reference query
            query = helper.filterQuery(query);
            this.db[locale].count(query, function (err, count) {
                // Maintaining context state
                process.domain = domain_state;
                try {
                    if (error) {
                        throw error;
                    } else {
                        // Is there necessity to generateCTNotFound error? We can use flags to indicate non-existence
                        return callback(null, {entries: count});
                    }
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Query parameter should be an object & not empty');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Method to insert data into DB
 * @param  {Object}   data      : Object containing details of inserting object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}           : Function to be called once this method is executed
 */

nedbStorage.prototype.insert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);

        if (_.isPlainObject(_data) && !_.isEmpty(_data) && _data._content_type_uid && _data.locale && _data._uid) {
            var content_type = _data._content_type_uid,
                locale = _data.locale;

            fileStorage.insert(_data, function (error, result) {
                // Maintaining context state
                process.domain = domain_state;
                try {
                    if (error) throw error;
                    // Since FS handles updating it
                    if (cachedStructures.indexOf(content_type) !== -1) {
                        return callback(null, result);
                    } else {
                        // remove the unwanted keys from the local-storage data
                        _data = helper.filterQuery(_data, true);
                        _data._uid = _data._uid || _data._data.uid;
                        self.db[locale].insert(_data, function (error) {
                            return callback(error, result);
                        });
                    }
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Data should be an object with at least `content_type_id` and `_uid`');
        }
    } catch (error) {
        return callback(error, null);
    }
};

/**
 * If object doesn't exist, insert. Else, update
 * @param  {Object}   data      : Object containing details of upserting object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}           : Function to be called once this method is executed
 */
// TODO: upsert & insert share too many similarities, short circuit them onto one?

nedbStorage.prototype.upsert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);

        if (_.isPlainObject(_data) && !_.isEmpty(_data) && _data._content_type_uid && _data.locale && _data._uid) {
                var locale = _data.locale,
                    content_type = _data._content_type_uid;

            fileStorage.upsert(_data, function (error, result) {
                try {
                    if (error) throw error;
                    // Since FS handles updating it
                    if (cachedStructures.indexOf(content_type) !== -1) {
                        return callback(null, result);
                    } else {
                        // remove the unwanted keys
                        _data = helper.filterQuery(_data, true);
                        _data._uid = _data._uid || _data._data.uid;
                        self.db[locale].update({_uid: _data._uid}, _data, {upsert: true}, function (error) {
                            // Maintaining context state
                            process.domain = domain_state;
                            return callback(error, result);
                        });
                    }
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Data should be an object with at least `content_type_id` and `_uid`');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Delete specific 'objects' from the specified 'Content type'
 * @param  {Object}   query     : Query object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */
// TODO: add option for multiple delete based on identifier OR filter

nedbStorage.prototype.remove = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale) {
            var locale = _query.locale,
                content_type = _query._content_type_uid;

            fileStorage.remove(_query, function (error, result) {
                if (error) throw error;
                // FS handles it
                if (cachedStructures.indexOf(content_type) !== -1) {
                    return callback(null, result);
                } else {
                    // to remove the unwanted keys from query and create reference query
                    _query = helper.filterQuery(_query);
                    // Remove Content Type from DB
                    // TODO: Check what format this one accepts for multiple deletion
                    self.db[locale].remove(_query, {}, function (error) {
                        // Maintaining context state
                        process.domain = domain_state;
                        return callback(error, result);
                    });
                }
            });
        } else {
            throw new Error('Query parameter should be an `object` and not empty!');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Bulk insert 'objects' into the specified 'Content Type'
 * @param  {Object}   query     : Query object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */

nedbStorage.prototype.bulkInsert = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        // TODO: _query.entries & objekts needs to be re-named to something more generic
        // TODO: Check is the json passed is correct
        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && (_.isArray(_query.entries) || _.isArray(_query.assets))) {
            var content_type = _query._content_type_uid;

            // Update persistent storage first
            fileStorage.bulkInsert(_query, function (error, result) {
                try {
                    if (error) throw error;
                    // FS handles it
                    if (cachedStructures.indexOf(content_type) !== -1) {
                        return callback(null, result);
                    } else {
                        var objekts = _query.entries || _query.assets || [],
                            locale = _query.locale,
                            _objekts = [];

                        for (var i = 0, total = objekts.length; i < total; i++) {
                            _objekts.push(function (objekt) {
                                return function (cb) {
                                    self.db[locale].update({_uid: (objekt.uid || objekt.entry.uid)}, {
                                        _data: objekt,
                                        _uid: (objekt.uid || objekt.entry.uid),
                                        _content_type_uid: content_type
                                    }, {upsert: true}, cb);
                                }
                            }(objekts[i]));
                        }
                        async.parallelLimit(_objekts, 5, callback);
                    }
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Query should be an object with at least `content_type_id` and `entries`');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Bulk delete objects from specified 'Content Type'
 * @param  {query}   query      : Query object
 * @param  {Function} callback  : Method to be called once the process is finished {type: (error, data)}
 */

/**
 * TODO: Add filter options to bulkDelete operation
 * Current format:
 * _query:{
 *  _content_type_uid: '',
 *  locale: '',
 *  objekts: {
 *   identifier1: ['values'],
 *   identifier2: ['values']
 *  }
 * }
 */

nedbStorage.prototype.bulkDelete = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && _.isPlainObject(_query.objekts)) {
            var content_type = _query._content_type_uid,
                locale = _query.locale;

            // Update persistent storage first
            fileStorage.bulkDelete(_query, function (error, result) {
                try {
                    if (error) throw error;
                    if (cachedStructures.indexOf(content_type) !== -1) {
                        return callback(null, result);
                    } else {
                        // Delete all keys except '_content_type_uid', 'locale', 'objekts'
                        // TODO: accommodate query filters on this
                        var req_keys = ['_content_type_uid', 'locale', 'objekts'],
                            keys = Object.keys(_query);

                        keys.forEach(function (key) {
                            // Current key is not part of required keys
                            if(req_keys.indexOf(key) === -1)
                                delete _query[key];
                        });

                        var _identifiers = Object.keys(_query.objekts),
                            obj = {};

                        /**
                         * Structure of documents in nedb
                         * [locale]: [
                         *   {
                         *    _data: {
                         *     // Entry details
                         *    },
                         *    locale: [locale],
                         *    _content_type_uid: [_content_type_uid]
                         *   }
                         * ]
                         * 
                         */
                        _identifiers.forEach(function (_identifier) {
                            obj['_data.' + _identifier] = {'$in': _query.objekts[_identifier]};
                        });

                        obj._content_type_uid = _query._content_type_uid,
                        obj.locale = _query.locale;

                        self.db[locale].remove(obj, function (removeError) {
                            if(removeError) {
                                console.trace(removeError);
                                throw removeError;
                            }
                            return callback(null, 1);
                        });
                    }
                } catch (error) {
                    return callback(error, null);
                }
            })
        } else {
            throw new Error('Invalid `query` object in bulkDelete. Kindly check the arguments passed!');
        }
    } catch (error) {
        return callback(error, null);
    }
}
exports = module.exports = new nedbStorage();