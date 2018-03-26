/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */

var _ = require('lodash'),
    when = require('when'),
    fs = require('fs'),
    path = require('path'),
    async = require('async'),
    context = require('./../context'),
    config = require('./../config/index'),
    Result = require('./query-builder/result'),
    languages = config.get('languages'),
    skipFormIds = ["_routes", "_content_types"],
    utility = {};

module.exports = exports = utility;

/*
 * generateCTNotFound
 * @description  : generateCTNotFound generates ContentType Error not found.
 * @params       : locale     {string} - locale
 *                 contentTypeUID {string} - contentTypeUID
 * @return       : isCyclic {boolean}
 */
exports.generateCTNotFound = function (locale, contentTypeUID) {
    var model = utility.getContentPath(locale);
    var jsonPath = path.join(model, contentTypeUID + ".json");
    var error = null;

    if (!jsonPath || !fs.existsSync(jsonPath)) {
        error = new Error("The Content Type uid '" + contentTypeUID + "' was not found or is invalid");
        error.code = 422;
        throw error;
    }
};

/*
 * checkCyclic
 * @description  : checkCyclic is used to determine the cyclic reference in given mapping
 * @params       : uid     {string} - uid to be avaluated
 *                 mapping {object} - mapping from which it is to be searched
 * @return       : isCyclic {boolean}
 */
exports.checkCyclic = function checkCyclic(uid, mapping) {
    var flag = false
    var list = [uid]
    var getParents = function (child) {
        let parents = []
        for (let key in mapping) {
            if (~mapping[key].indexOf(child)) {
                parents.push(key)
            }
        }
        return parents
    }
    for (let i = 0; i < list.length; i++) {
        var parent = getParents(list[i])
        if (~parent.indexOf(uid)) {
            flag = true
            break
        }
        list = _.uniq(list.concat(parent))

    }
    return flag
}

// _query to overrite the search on reference
exports.filterQuery = function (_query, build) {
    // remove the unwanted keys from the query
    var __keys = ["locale", "_remove", "include_references", "include_count", "_include_previous", "_include_next"];
    for (var i = 0, total = __keys.length; i < total; i++) {
        delete _query[__keys[i]];
    }

    // search for the reference
    var _filterQuery = function (query) {
        for (var key in query) {
            var _keys = (key) ? key.split('.') : [],
                _index = (_keys && _keys.length) ? _keys.indexOf('uid') : -1;
            if (_index > 1) {
                var _value = query[key];
                _keys[_index] = "values";
                var _key = _keys.join('.');
                query[_key] = _value;
                delete query[key];
            } else if (query[key] && typeof query[key] == "object") {
                _filterQuery(query[key]);
            }
        }
    };

    if (!build) _filterQuery(_query);
    return _query;
};

exports.merge = function (destination, source) {
    if (source && destination) {
        for (var key in source) {
            if (source[key]) destination[key] = source[key];
        }
    }
    return destination;
}

/**
 * Trim Content Type schema according to requirement
 * @param  {Object} _forms      : Collection of Content Types
 * @param  {Object} _remove     : Array of keys, that need to be deleted
 * @param  {String} key         : Collection object's key, on which the trimming is to be performed
 * @return {Object}             : Trimmed collection of objects
 */

exports.filterSchema = function (_forms, _remove, key) {
    var _keys = ['schema'].concat(_remove || []);
    var _removeKeys = function (object) {
        for (var i = 0, total = _keys.length; i < total; i++) {
            delete object[_keys[i]];
        }
        return object;
    };

    if (_forms && _forms instanceof Array) {
        for (var i = 0, total = _forms.length; i < total; i++) {
            if (key && typeof key === 'string') {
                if(_forms[i] && _forms[i][key])
                    _forms[i][key] = _removeKeys(_forms[i][key]);
            } else {
                if(_forms[i])
                    _forms[i] = _removeKeys(_forms[i]);
            }
        }
    } else if (_.isPlainObject(_forms)) {
        if (key && typeof key === 'string')
            _forms[key] = _removeKeys(_forms[key]);
        else
            _forms = _removeKeys(_forms);
    }
    return _forms;
};


// ~Modified :: findReferences method now accepts parent key
/**
 * Index references of the provided Content Type
 * @param  {Object} contentType : Content Type object who's references are to be indexed
 * @param  {String} parent      : Key that's to be prefixed to the references found
 * @return {Object}             : Content Type object, with attached indexed references
 */

exports.findReferences = function (contentType, parent) {
    if (contentType && contentType.schema && contentType.schema.length) {
        var _data = {},
            // Remove keys that do not match the list
            _keys = ["title", "uid", "schema", "options", "singleton", "references", "created_at", "updated_at", "locale", "_content_type_uid"];

        var _removeKeys = function (contentType) {
            for (var _field in contentType) {
                if (_keys.indexOf(_field) == -1) {
                    delete contentType[_field];
                }
            }
            return contentType;
        }

        var _findReferences = function (_schema, _parent) {
            for (var i = 0, total = _schema.length; i < total; i++) {
                var parentKey;
                if (_schema[i] && _schema[i]['data_type'] && _schema[i]['data_type'] == "reference") {
                    var field = ((_parent) ? _parent + ":" + _schema[i]['uid'] : _schema[i]['uid']);
                    _data[field] = _schema[i]['reference_to'];
                } else if (_schema[i] && _schema[i]['data_type'] && _schema[i]['data_type'] == "group" && _schema[i]['schema']) {
                    _findReferences(_schema[i]['schema'], ((_parent) ? _parent + ":" + _schema[i]['uid'] : _schema[i]['uid']));
                }
            }
        };

        contentType = _removeKeys(contentType);
        // ~Modified :: findReferences method now accepts parent key
        _findReferences(contentType.schema, parent);
        // adding or recalculating the references of the form
        contentType['references'] = _data;
    }
    return contentType;
};


exports.filterEntries = function (content_type_id, fields, _entries, key) {
    if (_entries && fields && fields.length) {
        var _default = ['uid'];

        fields = _.uniq(fields.concat(_default));

        var _filterData = function (_entry) {
            if (key && typeof key === 'string') {
                var entry = {"_uid": _entry["_uid"], "_content_type_uid": content_type_id};
                entry[key] = {};
                for(var f = 0, _f = fields.length; f < _f; f++) {
                    entry[key][fields[f]] = _entry[key][fields[f]];
                }
            } else {
                var entry = {"_uid": _entry["_uid"], "_content_type_uid": content_type_id};
                for(var f = 0, _f = fields.length; f < _f; f++) {
                    entry[fields[f]] = _entry[fields[f]];
                }
            }
            return entry;
        };

        if (_entries instanceof Array) {
            for (var i = 0, total = _entries.length; i < total; i++) {
                _entries[i] = _filterData(_entries[i]);
            }
        } else if (_entries && typeof _entries == "object") {
            _entries = _filterData(_entries);
        }
    }
    return _entries;
};

exports.queryBuilder = function (query, language, content_type_id, callback) {
    var skipFormIds = ["_routes", "_content_types", "_assets"];
    if (query && Object.keys(query).length && content_type_id && skipFormIds.indexOf(content_type_id) === -1) {
        var Inmemory = require('./inmemory/index'),
            schema = Inmemory.get(language, "_content_types", {_uid: content_type_id}),
            references = {};

        if (schema && schema.length) {
            schema = schema[0];
            references = schema.references || {};
        }

        // check if the reference exists in the system
        if (Object.keys(references).length > 0) {
            var requests = [];
            for (var filterField in query) {
                requests.push(function (filterField) {
                    return function (_callback) {
                        var _calls = {};
                        var _filterField = filterField.toString();
                        var refQuery, refForm;

                        for (var refField in references) {
                            var newRefField = refField.replace(/:/g, ".");
                            if (filterField.indexOf(newRefField) === 0) {
                                // processing the new query param
                                _filterField = _filterField.split('.');
                                _filterField[_filterField.length - 1] = "uid";
                                _filterField = _filterField.join(".");

                                refForm = references[refField];
                                refQuery = refQuery || {};
                                var newFilterField = filterField.replace(newRefField, "_data");  // remove this entry, replacement if system going to attach the "_data."
                                refQuery[newFilterField] = query[filterField];
                                delete query[filterField];
                            }
                        }

                        if (refQuery && Object.keys(refQuery).length) {
                            _calls[_filterField] = (function (refQuery, content_type_id) {
                                return function (_cb) {
                                    var RefData = Inmemory.get(language, content_type_id, refQuery),
                                        RefQuery = {"$in": []};
                                    if (RefData && RefData.length) RefQuery = {"$in": _.map(RefData, "uid")};
                                    _cb(null, RefQuery);
                                }
                            }(refQuery, refForm));
                        } else if (_.isArray(query[filterField])) {
                            var __calls = [];
                            for (var i = 0, total = query[filterField].length; i < total; i++) {
                                __calls.push(function (filterQuery) {
                                    return function (__cb) {
                                        utility.queryBuilder(filterQuery, language, content_type_id, __cb);
                                    }
                                }(query[filterField][i]));
                            }

                            _calls[filterField] = (function (__calls) {
                                return function (_cb) {
                                    async.parallel(__calls, _cb);
                                }
                            }(__calls));
                        }

                        if (Object.keys(_calls).length) {
                            async.parallel(_calls, _callback);
                        } else {
                            var _temp = {};
                            _temp[filterField] = query[filterField];
                            _callback(null, _temp);
                        }
                    }
                }(filterField));
            }

            async.parallel(requests, function (err, result) {
                var __query = {};
                for (var i = 0, total = result.length; i < total; i++) {
                    _.merge(__query, result[i]);
                }
                callback(null, __query);
            });
        } else {
            callback(null, query);
        }
    } else {
        callback(null, query);
    }
};

// generate the Result object
/**
 * Wrap result, when toJSON() hasn't been invoked in query builder
 * @param  {Object} result  : Object to be wrapped
 * @return {Object}         : Wrapped Object
 */

exports.resultWrapper = function (result) {
    // On find() query
    if (result && typeof result.entries !== 'undefined') {
        // find() resulted in no. of entries
        if (result.entries && result.entries.length) {
            for (var i = 0, _i = result.entries.length; i < _i; i++) {
                result.entries[i] = Result(result.entries[i]);
            }
        // count() invoked in find()
        } else if (result.entries && typeof result.entries === 'number') {
            result = {entries: result.entries};
        // find() resulted in an empty array
        } else {
            result.entries = [];
        }
    // On find() assets
    } else if (result && typeof result.assets !== 'undefined') {
        // find() resulted in no. of assets
        if (result.assets && result.assets.length) {
            for (var j = 0, _j = result.assets.length; j < _j; j++) {
                result.assets[j] = Result(result.assets[j]);
            }
        // find() resulted in an empty array
        } else {
            result.assets = [];
        }
    // On findOne() entry
    } else if (result && typeof result.entry !== 'undefined') {
        // findOne() resulted in single entry
        // TODO: This might break?
        result.entry = Result(result.entry);
    // On findOne() asset
    } else if (result && typeof result.asset !== 'undefined') {
        // findOne() resulted in single asset
        // TODO: This might break?
        result.asset = Result(result.asset);
    } else {
        result = {"_write_operation_": result};
    }
    return result;
};

// spread the result object
/**
 * Adds array ([]) wrapper over collection, while keeping singular 'objects' intact
 * @param  {Object} result  : Object that's to be spread
 * @return {Object}         : Spread object
 */

exports.spreadResult = function (result) {
    try {
        var _results = [];
        // TODO: update conditions
        if (result && typeof result === "object" && Object.keys(result).length) {
            // If its find() for entries
            if (typeof result.entries !== 'undefined') {
                // If find() hasn't invoked count()
                if (typeof result.entries !== 'number') {
                    _results.push(result.entries);
                } else {
                    _results = result;
                }
            }
            if (typeof result.assets !== 'undefined') _results.push(result.assets);
            if (typeof result.asset !== 'undefined') _results = result.asset;
            if (typeof result.schema !== 'undefined') _results.push(result.schema);
            if (typeof result.count !== 'undefined') _results.push(result.count);
            if (typeof result.entry !== 'undefined') _results = result.entry;
            if (typeof result._write_operation_ !== 'undefined') _results = result._write_operation_;
        }
        return _results;
    } catch (error) {
        console.error('PK-', error);
        return [];
    }
};

exports.getContentPath = function (langCode) {
    var idx = _.findIndex(languages, {"code": langCode});
    if (~idx) {
        return languages[idx]['contentPath'];
    } else {
        console.error("Language doesn't exists");
    }
};

exports.getAssetPath = function (langCode) {
    var idx = _.findIndex(languages, {"code": langCode});
    if (~idx) {
        return languages[idx]['assetsPath'];
    } else {
        console.error("Language doesn't exists");
    }
};


// create the promise for the query
exports.getPromise = function (queryObject) {
    var datastore = require('./providers');
    var Inmemory = require('./inmemory');
    var deferred = when.defer();
    try {
        var result,
            options,
            _query,
            self = queryObject,
            isJson = (typeof self.json === 'function') ? true : false,
            isSingle = (self._uid || self.single) ? true : false,
            callback = function (operation) {
                return function (err, data) {
                    try {
                        // ??
                        self._locale = self._operation = self._uid = self.single = self.json = null;
                        if (err) throw err;
                        // If toJSON() has not been invoked, wrap result
                        if (!isJson) data = utility.resultWrapper(data);
                        data = utility.spreadResult(data);
                        console.log('PK data', data)
                        return deferred.resolve(data);
                    } catch (err) {
                        console.error('PK-', err);
                        return deferred.reject(err);
                    }
                }
            };
        /*
         * setting the locale, setting the options, setting the default sort option to publish_at descending
         */
        // TODO: fails if goes out of context
        self._locale = self._locale || context.get("lang") || "en-us";

        if (self._query._bulk_insert)
            self._operation = 'bulkInsert';
        if(self._query._bulk_delete)
            self._operation = 'bulkDelete';

        switch (self._operation) {
            case 'upsert':
            case 'remove':
            case 'insert':
                _query = _.cloneDeep(self.object);
                _query = _.merge(_query, {_content_type_uid: self.content_type_id, locale: self._locale});

                if (self._uid || (_query._data && _query._data.uid)) _query._uid = self._uid || _query._data.uid;
                datastore[self._operation](_query, callback(self._operation));
                break;
            case 'bulkInsert':
            case 'bulkDelete':
                _query = _.cloneDeep(self._query);
                _query = _.merge(_query, {_content_type_uid: self.content_type_id, locale: self._locale});
                datastore[self._operation](_query, callback(self._operation));
                break;
            case 'fetch' :
            case 'count' :
            case 'find' :
                options = self._options || {};
                options.sort = options.sort || {"_data.published_at": -1};
                _query = _.cloneDeep(self._query);

                utility.queryBuilder(_query, self._locale, self.content_type_id, function (err, resultQuery) {
                    if (err) throw err;
                    _query = resultQuery;

                    //creating query based on the chain methods
                    _query = _.merge(_query, {_content_type_uid: self.content_type_id, locale: self._locale});
                    if (self._uid) _query = _.merge(_query, {_uid: self._uid});

                    // if (skipFormIds.indexOf(self.content_type_id) === -1) {
                        if (self.include_count) _query.include_count = true;
                        if (self._uid || self.single) {
                            datastore.findOne(_query, callback(self._operation));
                        } else if (self._count) {
                            datastore.count(_query, callback(self._operation));
                        } else {
                            datastore.find(_query, options, callback(self._operation));
                        }
                    // } else {
                    //     var results = Inmemory.get(self._locale, self.content_type_id, _query);
                    //     // entry/entries are added because to get the data under the result wrapper
                    //     if (self._uid || self.single) {
                    //         results = (results && results.length) ? results[0] : null;
                    //         results = {'entry': results};
                    //     } else if (self._count) {
                    //         results = results || [];
                    //         results = {'entries': results.length};
                    //     } else {
                    //         results = {'entries': results || []};
                    //         if (self.include_count) results.count = results.entries.length;
                    //     }
                    //     callback(self._operation)(null, results);
                    // }
                });
                break;
        }
        return deferred.promise;
    } catch (err) {
        return deferred.reject(err);
    }
}