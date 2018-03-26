/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var sift            = require('sift'),
    path            = require('path'),
    fs              = require('graceful-fs'),
    events          = require('events').EventEmitter,
    util            = require('util'),
    _               = require('lodash'),
    async           = require('async'),
    config          = require('../../config'),
    helper          = require('../helper'),
    InMemory        = require('../inmemory'),
    languages       = config.get('languages'),
    assetDwldFlag   = config.get('assets.download'),
    cache           = config.get('cache'),
    assetRoute      = '_assets',
    entryRoute      = '_routes',
    schemaRoute     = '_content_types',
    cachedStructures = [assetRoute, entryRoute, schemaRoute];

var fileStorage = function () {
    // Inherit methods from EventEmitter
    events.call(this);
    // Remove memory-leak warning about max listeners
    this.setMaxListeners(0);
    // Keep track of spawned child processes
    this.childProcesses = [];
};

// Extend from base provider
util.inherits(fileStorage, events);

// include references
fileStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
    var self = this,
        calls = [];
    if (_.isEmpty(references)) references = {};
    var _includeReferences = function (data) {
        for (var _key in data) {
            if (data.uid) parentID = data.uid;
            if (_.isPlainObject(data[_key])) {
                if (data[_key] && data[_key]["_content_type_id"]) {
                    calls.push(function (_key, data) {
                        return (function (_callback) {
                            var _uid = (data[_key]["_content_type_id"] === assetRoute && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                            var query = {
                                    "_content_type_uid": data[_key]["_content_type_id"],
                                    "_uid": _uid,
                                    "locale": _locale,
                                    "_remove": true
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
                                            if (_data || (_data && _data.assets)) {
                                                var __data = [];
                                                if (query._uid && query._uid.$in) {
                                                    for (var a = 0, _a = query._uid.$in.length; a < _a; a++) {
                                                        var _d = _.find((_data.assets) ? _data.assets: _data, {uid: query._uid.$in[a]});
                                                        if (_d) __data.push(_d);
                                                    }
                                                    data[field] = __data;
                                                } else {
                                                    data[field] = (_data.assets && _data.assets.length) ? _data.assets[0] : {};
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
        return callback(e, null);
    }
};


/**
 * Find object based on query
 * @param  {Object}   query     : Query options
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */

fileStorage.prototype.findOne = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && _.isFunction(callback)) {
            var language = _query.locale,
                content_type = _query._content_type_uid,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                remove = _query._remove || false,
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true : false,
                jsonPath = path.join(model, content_type + '.json');

            // Delete unwanted keys
            // TODO: use _.omit instead
            _query = helper.filterQuery(_query);

            // Get '_assets', '_content_types', '_routes' details off InMemory
            if(cache && cachedStructures.indexOf(content_type) !== -1) {
                var results = Inmemory.get(language, content_type, _query),
                    data = (results && results.length) ? results[0]: [];
                data = (remove) ? data: ((content_type === assetRoute) ? {asset: data}: {entry: data});
                return callback(null, data);
            // Either cache is false, or the content type is for some entry
            } else if (fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, models) {
                    try {
                        if (error) throw error;
                        var data;
                        models = JSON.parse(models);
                        if (models && models.length) data = sift(_query, models);
                        if (data && data.length) {
                            _data = (remove) ? data[0]: ((content_type === assetRoute) ? {asset: _.cloneDeep(data[0]._data)}: {entry: _.cloneDeep(data[0]._data)});
                            if(content_type !== assetRoute && includeReference)
                                self.includeReferences(_data, language, undefined, undefined, callback);
                            else
                                return callback(null, _data);
                        } else {
                            var _data = (content_type === assetRoute) ? {asset: null}: {entry: null};
                            return callback(null, _data);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                });
            } else {
                // TODO: generateNOCTFound || need clarification
                var data = (content_type === assetRoute) ? {asset: null}: {entry: null};
                return callback(null, data);
            }
        } else {
            throw new Error('Query parameter should be an object & not empty, while the last parameter should be an error-first callback');
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
 * 1. Query: based on '_content_type_uid', 'locale' && _query
 * 2. Apply options: asc/desc/limit/skip TODO: need to do this for assets too!
 * 3. For entries, find references if required
 */

fileStorage.prototype.find = function (query, options, callback) {
    try {
        var self = this,
            _query = _.cloneDeep(query),
            references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3]: {},
            parentID = (_.isString(arguments[4])) ? arguments[4] : undefined;
        console.log('FS find()', query);
        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _.isPlainObject(options) && _query._content_type_uid && _query.locale && _.isFunction(callback)) {
            var sort = options.sort || {'_data.published_at': -1},
                language = _query.locale,
                remove = _query._remove || false,
                _count = _query.include_count || false,
                content_type = _query._content_type_uid,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true : false,
                jsonPath = path.join(model, _query._content_type_uid + '.json');

            // Delete unwanted keys
            // TODO: use _.omit instead
            _query = helper.filterQuery(_query);

            // TODO: add options like asc/desc/limit/skip here too
            if(cache && cachedStructures.indexOf(content_type) !== -1) {
                var results = Inmemory.get(language, content_type, _query),
                    data = (results && results.length) ? results: [];
                data = (remove) ? data: ((content_type === assetRoute) ? {assets: data}: {entries: data});
                return callback(null, data);
            } else if (fs.existsSync(jsonPath)) {
                // Read FS
                fs.readFile(jsonPath, 'utf-8', function (error, models) {
                    try {
                        if (error) throw error;
                        models = JSON.parse(models);
                        // TODO: _query builder should not have _data!!
                        if (models && models.length) models = sift(_query, models);
                        var _data = _.map(models, '_data') || [],
                            __data;

                        // TODO: Needs replacement
                        /* Sorting Logic */
                        var keys = Object.keys(sort),
                            __sort = {keys: [], order: []};
                        for (var i = 0, total = keys.length; i < total; i++) {
                            var __order = (sort[keys[i]] === 1) ? 'asc' : 'desc';
                            // removing the _data. key to make the default sorting work
                            __sort.keys.push(keys[i].replace('_data.', ''));
                            __sort.order.push(__order);
                        }
                        _data = _.sortByOrder(_data, __sort.keys, __sort.order);
                        /* Sorting Logic */

                        if (options.limit) {
                            options.skip = options.skip || 0;
                            _data = _data.splice(options.skip, options.limit);
                        } else if (options.skip > 0) {
                            _data = _data.slice(options.skip);
                        }
                        __data = (!remove) ? {entries: _data}: _data
                        if (_count) __data.count = _data.length;
                        console.log('FS find()', _query, includeReference);
                        if (includeReference) {
                            if (parentID) {
                                var tempResult = (!remove) ? __data.entries: __data;
                                references[parentID] = references[parentID] || [];
                                references[parentID] = _.uniq(references[parentID].concat(_.map(tempResult, "uid")));
                            }
                            self.includeReferences(__data, language, references, parentID, function (error, results) {
                                return callback(error, results);
                            });
                        } else {
                            return callback(null, __data);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                }); 
            } else {
                // TODO: generateNOCTFound || need clarification
                var data = (remove) ? {}: ((content_type === assetRoute) ? {assets: []}: {entries: []});
                return callback(null, data);   
            }
        } else {
            throw new Error('Query & options parameter should be of type `object` and `query` object should not be empty, while last parameter should be an error-first callback');
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

fileStorage.prototype.count = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query)) {
            self.find(_query, {sort: {'_data.published_at': -1}}, function (error, data) {
                try {
                    // Maintaining context state
                    process.domain = domain_state;
                    if (error) throw error;
                    return callback(null, {entries: data.entries.length});
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
 * Method to insert data into FS, based on 'Content Type' & 'locale' passed
 * @param  {Object}   data      : Object containing details of inserting object
 * @param  {Function} callback  : Error-first callback
 * @return {Function}           : Function to be called once this method is executed
 */

fileStorage.prototype.insert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);

        if (_.isPlainObject(_data) && !_.isEmpty(_data) && _data._content_type_uid && _data.locale && _data._uid) {
            var language = _data.locale,
                content_type = _data._content_type_uid,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, content_type + '.json');

            // Delete unwanted keys
            // TODO: use _.omit instead
            _data = helper.filterQuery(_data, true);

            var _callback = function (_entries) {
                fs.writeFile(jsonPath, JSON.stringify(_entries), function (error) {
                    // Maintaining context state
                    process.domain = domain_state;
                    return callback(error, 1);
                });
            };

            // updating the references based on the new schema
            // ~Modified :: findReferences method now accepts parent key
            if (content_type === schemaRoute) _data['_data'] = helper.findReferences(_data['_data'], '_data');
            if (fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, objekts) {
                    if (error) throw error;
                    objekts = JSON.parse(objekts) || [];
                    var idx = _.findIndex(objekts, {_uid: _data._uid});
                    if (~idx) {
                        return callback(new Error('Data already exists, use update instead of insert'), null);
                    } else {
                        if (cache) InMemory.set(language, content_type, _data._uid, _data);
                        objekts.unshift(_data);
                        _callback(objekts);
                    }
                });
            } else {
                if (cache) InMemory.set(language, content_type, _data._uid, _data);
                _callback([_data]);
            }
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
fileStorage.prototype.upsert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);

        if (_.isPlainObject(_data) && !_.isEmpty(_data) && _data._content_type_uid && _data.locale && _data._uid) {
            var content_type = _data._content_type_uid,
                language = _data.locale,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, content_type + '.json');

            // to remove the unwanted keys from query/data and create reference query
            _data = helper.filterQuery(_data, true);

            var _callback = function (__data) {
                fs.writeFile(jsonPath, JSON.stringify(__data), function (error) {
                    // Maintaining context state
                    process.domain = domain_state;
                    return callback(error, 1);
                });
            };

            // updating the references based on the new schema
            // ~Modified :: findReferences method now accepts parent key
            if (content_type === schemaRoute) _data['_data'] = helper.findReferences(_data['_data'], '_data');
            if (fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, objekts) {
                    if (error) throw error;
                    objekts = JSON.parse(objekts);
                    var idx = _.findIndex(objekts, {_uid: _data._uid});
                    if (idx !== -1) objekts.splice(idx, 1);
                    objekts.unshift(_data);
                    if (cache) InMemory.set(language, content_type, _data._uid, _data);
                    _callback(objekts);
                });
            } else {
                if (cache) InMemory.set(language, content_type, _data._uid, _data);
                _callback([_data]);
            }
        } else {
            throw new Error('Data should be an object with at least `content_type_id` and `_uid`');
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

fileStorage.prototype.bulkInsert = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        // TODO: _query.entries & objekts needs to be re-named to something more generic
        // TODO: Check is the json passed is correct
        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && (_.isArray(_query.entries) || _.isArray(_query.assets))) {
            var objekts = _query.entries || _query.assets || [],
                content_type = _query._content_type_uid,
                language = _query.locale,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, content_type + '.json'),
                _objekts = [];

            if(jsonPath && fs.existsSync(jsonPath)) {
                for (var i = 0, total = objekts.length; i < total; i++) {
                    _objekts.push({
                        _data: objekts[i],
                        _content_type_uid: content_type,
                        _uid: objekts[i]['uid'] || objekts[i]['entry']['uid'] // entry is just for the _routes
                        // TODO: need to observe this
                    });
                }

                fs.writeFile(jsonPath, JSON.stringify(_objekts), function (error) {
                    // Maintaining context state
                    process.domain = domain_state;
                    if (error) return callback(error, null);
                    if(cache) InMemory.set(language, content_type, null, _objekts);
                    return callback(null, true);
                });
            } else{
                // Provided Content Type's path does not exist
                throw new Error('Path not found for ' + content_type);
            }
        } else {
            throw new Error('Query should be an object with `content_type_id {string}`, `locale {string}` and `entries {object}`');
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
// Added option for multiple delete objects
// TODO: Add option for multiple delete based on user defined identifier OR filter
fileStorage.prototype.remove = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale) {
            var language = _query.locale,
                content_type = query._content_type_uid,
                model = (content_type !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, content_type + '.json');

            // if the object to be removed does not exist in that path
            if (!fs.existsSync(jsonPath)) {
                return callback(null, 1);
            } else {
                // Removing Content Type object
                if (Object.keys(_query).length === 2 && content_type && language) {
                    fs.unlink(jsonPath, function (error) {
                        if (error) throw error;
                        InMemory.set(language, content_type, null, []);

                        // Removing the specified 'content_type' uid from '_routes'
                        var _q = {
                            _content_type_uid: entryRoute,
                            locale: language
                        };
                        // Fetch from InMemory || eval disadvantages of getting from Inmemory
                        self.find(_q, {}, function (error, result) {
                            if(result && result.entries && result.entries.length > 0) {
                                _q.entries = _.reject(result.entries, {content_type: { uid: content_type}});
                                self.bulkInsert(_q, callback);
                            } else {
                                return callback(null, 1);
                            }
                        });

                    });
                // TODO: Conditions need to be updated
                } else if (content_type) {
                    var idx, entries, idxData;
                    // remove the unwanted keys from query/data
                    _query = helper.filterQuery(_query);

                    fs.readFile(jsonPath, 'utf-8', function (error, entries) {
                        if (error) throw error;
                        entries = JSON.parse(entries);
                        if(typeof _query._uid === 'string') {
                            // Make this generic: support other identifiers other than '_uid'
                            entries = _.reject(entries, {_uid: _query._uid});
                            fs.writeFile(jsonPath, JSON.stringify(entries), function (err) {
                                if (err) throw err;
                                // If cache, then update Inmemory
                                if(cache)
                                    InMemory.set(language, content_type, _query._uid);
                                return callback(null, 1);
                            });
                        } else if (_.isArray(_query._uid)) {
                            // If there are multiple 'remove' objects
                            _query._uid.forEach(function (id) {
                                entries = _.reject(entries, {'_uid': id});
                            });
                            fs.writeFile(jsonPath, JSON.stringify(entries), function (writeError) {
                                if(writeError)
                                    throw writeError;
                                // TODO: Add check if Inmemory set is required
                                _query._uid.forEach(function (id) {
                                    InMemory.set(language, content_type, id);
                                });
                                return callback(null, 1);
                            });
                        } else {
                            // It shouldn't have had come here
                            console.trace(_query);
                            throw new Error('Invalid `_query._uid` parameters passed');
                        }
                    });
                } else {
                    return callback(null, 0);
                }
            }
        } else {
            throw new Error('Query parameter should be an `object` and not empty!');
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

fileStorage.prototype.bulkDelete = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid && _query.locale && _.isPlainObject(_query.objekts)) {
            var _q = {
                    _content_type_uid: _query._content_type_uid,
                    locale: _query.locale,
                    includeReferences: false
                };
            // Perform the lookup operation on the specified 'Content Type | Collection'
            self.find(_q, {}, function (error, data) {
                // Maintaining context state
                process.domain = domain_state;
                if(error)
                    throw error;
                if(data.entries || data.assets) {
                    var filteredData = (data.entries) ? _.cloneDeep(data.entries): _.cloneDeep(data.assets),
                        _identifiers = Object.keys(_query.objekts);
                    // TODO: add querying here
                    _identifiers.forEach(function (_identifier) {
                        if(_.isArray(_query.objekts[_identifier])) {
                            // Delete all values for the specified key
                            _query.objekts[_identifier].forEach(function (id) {
                                var obj = {};
                                obj[_identifier] = id;
                                filteredData = _.reject(filteredData, obj);
                            });
                        }
                    });
                    _q.entries = filteredData;
                    // Now bulk insert the data
                    self.bulkInsert(_q, callback);
                } else {
                    // The queried 'Content Type | Collection' did not have any data in them
                    return callback(null, 1);
                }
            });
        } else {
            throw new Error('Invalid `query` object in bulkDelete. Kindly check the arguments passed!');
        }
    } catch (error) {
        return callback(error, null);
    }
}


// custom sort function
fileStorage.prototype.sortByKey = function (array, key, asc) {
    var _keys = key.split('.'),
        len = _keys.length;

    return array.sort(function (a, b) {
        var x = a, y = b;
        for (var i = 0; i < len; i++) {
            x = x[_keys[i]];
            y = y[_keys[i]];
        }
        if (asc) {
            return ((x < y) ? -1 : ((x > y) ? 1 : 0));
        }
        return ((y < x) ? -1 : ((y > x) ? 1 : 0));
    });
};

exports = module.exports = new fileStorage();