/**
 * Module dependencies.
 */
var path = require('path'),
    fs = require('graceful-fs'),
    url = require('url'),
    _ = require('lodash'),
    pathToRegexp = require('path-to-regexp'),
    mkdirp = require('mkdirp'),
    request = require('request'),
    async = require('async'),
    utils = require('./../utils/index'),
    pkg = require('./../../package.json'),
    db = utils.db,
    AWS = require('aws-sdk');

var config = utils.config,
    languages = config.get('languages'),
    _types = config.get('contentstack.types'),
    assetDownloadFlag = config.get('assets.download'),
    headers = {
        api_key: config.get('contentstack.api_key'),
        access_token: config.get('contentstack.access_token'),
        'X-User-Agent': 'contentstack-cli/' + pkg.version
    },
    assetsConf = config.get('assets');


var storageProvider = config.get('storage.provider')
// start to AWS Configuration
AWS.config = new AWS.Config();
AWS.config.accessKeyId = "AKIAIGPNSN2M3BK4VP6Q";
AWS.config.secretAccessKey = "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";
var bucketName = 'csassetsbucket',
    s3 = new AWS.S3()
// End to AWS configuration

var helper = module.exports = {};

// create all directories as per path
helper.mkdirAllSync = function (path, permission) {
    mkdirp.sync(path, permission);
};

// remove extra and unwanted keys from entry object
helper.deleteKeys = function (entry) {
    var keys = ["ACL", "publish_details"],
        entry = entry.object || entry,
        d = new Date();

    entry.uid = (entry._metadata && entry._metadata.uid) ? entry._metadata.uid : entry.uid;
    entry.published_at = d.toISOString();
    return _.omit(entry, keys);
};

// update references in entry object
helper.updateReferences = function (data) {
    if (data && data.schema && data.entry) {
        var parent = [];
        var update = function (parent, form_id, entry) {
            var _entry = entry,
                len = parent.length;
            for (var j = 0; j < len; j++) {
                if (_entry && parent[j]) {
                    if (j == (len - 1) && _entry[parent[j]]) {
                        if (form_id !== '_assets') {
                            _entry[parent[j]] = {values: _entry[parent[j]], _content_type_id: form_id};
                        } else {
                            if (_entry[parent[j]] instanceof Array) {
                                var assetIds = [];
                                for (var k = 0; k < _entry[parent[j]].length; k++) {
                                    assetIds.push(_entry[parent[j]][k]['uid'])
                                }
                                _entry[parent[j]] = {values: assetIds, _content_type_id: form_id};
                            } else {
                                _entry[parent[j]] = {values: _entry[parent[j]]['uid'], _content_type_id: form_id};
                            }
                        }
                    } else {
                        _entry = _entry[parent[j]];
                        var _keys = _.cloneDeep(parent).splice(eval(j + 1), len);
                        if (_entry instanceof Array) {
                            for (var i = 0, _i = _entry.length; i < _i; i++) {
                                update(_keys, form_id, _entry[i]);
                            }
                        } else if (!_entry instanceof Object) {
                            break;
                        }
                    }
                }
            }
        };
        var find = function (schema, entry) {
            for (var i = 0, _i = schema.length; i < _i; i++) {
                if (schema[i].data_type == "reference") {
                    parent.push(schema[i].uid);
                    update(parent, schema[i].reference_to, entry);
                    parent.pop();
                }
                if (!assetDownloadFlag && schema[i].data_type == "file") {
                    parent.push(schema[i].uid);
                    update(parent, '_assets', entry);
                    parent.pop();
                }
                if (schema[i].data_type == "group") {
                    parent.push(schema[i].uid);
                    find(schema[i].schema, entry);
                    parent.pop();
                }
            }
        };
        find(data.schema, data.entry);
    }
    return data;
};

// replace assets url
helper.replaceAssetsUrl = function (_assets, content_type, entry) {
    if (content_type && content_type.schema && entry) {
        var parent = [];
        var replace = function (parent, schema, entry) {
            var _entry = entry,
                len = parent.length;
            for (var j = 0; j < len; j++) {
                if (j == (len - 1) && _entry[parent[j]]) {
                    if (_entry[parent[j]] instanceof Array) {
                        for (var i = 0, _i = _entry[parent[j]].length; i < _i; i++) {
                            replace([i], schema, _entry[parent[j]]);
                        }
                    } else {
                        switch (schema.data_type) {
                            case "file":
                                _entry[parent[j]] = _assets[_entry[parent[j]].uid];
                                break;
                            case "text":
                                var _matches, regex, __entry;
                                //for the old contentstack
                                if (schema && schema.field_metadata && schema.field_metadata.markdown) {
                                    regex = new RegExp('https://(dev-new-|stag-new-|)(contentstack-|)api.(built|contentstack).io/(.*?)/download(.*?)uid=([a-z0-9]+[^&\?\s\n])((.*)[\n\s]?)', 'g');
                                } else {
                                    regex = new RegExp('https://(dev-new-|stag-new-|)(contentstack-|)api.(built|contentstack).io/(.*?)/download(.*?)uid=([a-z0-9]+[^\?&\'"])(.*?)', 'g');
                                }
                                __entry = _entry[parent[j]].slice(0);
                                while ((_matches = regex.exec(_entry[parent[j]])) !== null) {
                                    if (_matches && _matches.length) {
                                        var download_id = url.parse(_matches[0]).pathname.split('/').slice(1).join('/'),
                                            obj = _assets[download_id];
                                        if (obj && obj['url'] && obj['url'] == _matches[0]) __entry = (schema && schema.field_metadata && schema.field_metadata.markdown) ? __entry.replace(_matches[0], encodeURI(obj._internal_url) + "\n") : __entry.replace(_matches[0], obj._internal_url);
                                    }
                                }
                                _entry[parent[j]] = __entry;

                                //for the new contentstack
                                var _matches2, regex2, __entry2;
                                if (schema && schema.field_metadata && schema.field_metadata.markdown) {
                                    regex2 = new RegExp('https://(dev-|stag-|)(assets|images).contentstack.io/v[\\d]/assets/(.*?)/(.*?)/(.*?)/download', 'g');
                                } else {
                                    regex2 = new RegExp('https://(dev-|stag-|)(assets|images).contentstack.io/v[\\d]/assets/(.*?)/(.*?)/(.*?)/download', 'g');
                                }
                                __entry2 = _entry[parent[j]].slice(0);
                                while ((_matches2 = regex2.exec(_entry[parent[j]])) !== null) {
                                    if (_matches2 && _matches2.length) {
                                        var download_id = url.parse(_matches2[0]).pathname.split('/').slice(4).join('/'),
                                            _obj = _assets[download_id];
                                        if (_obj && _obj['url'] && _obj['url'] == _matches2[0]) __entry2 = (schema && schema.field_metadata && schema.field_metadata.markdown) ? __entry2.replace(_matches2[0], encodeURI(_obj._internal_url) + "\n") : __entry2.replace(_matches2[0], _obj._internal_url);
                                    }
                                }
                                _entry[parent[j]] = __entry2;
                                break;
                        }
                    }
                } else {
                    _entry = _entry[parent[j]];
                    var _keys = _.cloneDeep(parent).splice(eval(j + 1), len);
                    if (_entry instanceof Array) {
                        for (var i = 0, _i = _entry.length; i < _i; i++) {
                            replace(_keys, schema, _entry[i]);
                        }
                    } else if (typeof _entry != "object") {
                        break;
                    }
                }
            }
        };
        var find = function (schema, entry) {
            for (var i = 0, _i = schema.length; i < _i; i++) {
                if ((assetDownloadFlag && schema[i].data_type == "file") || (schema[i].data_type == "text")) {
                    parent.push(schema[i].uid);
                    replace(parent, schema[i], entry);
                    parent.pop();
                }
                if (schema[i].data_type == "group") {
                    parent.push(schema[i].uid);
                    find(schema[i].schema, entry);
                    parent.pop();
                }
            }
        };
        find(content_type.schema, entry);
        return entry;
    }
};

// get assets object
helper.getAssetsIds = function (data) {
    if (data && data.content_type && data.content_type.schema && data.entry) {
        var parent = [],
            assetsIds = [];
        var _get = function (schema, _entry) {
            switch (schema.data_type) {
                case "file":
                    if (_entry && _entry.uid) {
                        assetsIds.push(_entry);
                    }
                    break;
                case "text":
                    var _matches, regex;
                    if (schema && schema.field_metadata && schema.field_metadata.markdown) {
                        regex = new RegExp('https://(dev-new-|stag-new-|)(contentstack-|)api.(built|contentstack).io/(.*?)/download(.*?)uid=([a-z0-9]+[^\?&\s\n])((.*)[\n\s]?)', 'g');
                    } else {
                        regex = new RegExp('https://(dev-new-|stag-new-|)(contentstack-|)api.(built|contentstack).io/(.*?)/download(.*?)uid=([a-z0-9]+[^\?&\'"])(.*?)', 'g');
                    }
                    while ((_matches = regex.exec(_entry)) !== null) {
                        if (_matches && _matches.length) {
                            var assetObject = {};
                            if (_matches[6]) assetObject['uid'] = _matches[6];
                            if (_matches[0]) {
                                assetObject['url'] = _matches[0];
                                assetObject['download_id'] = url.parse(_matches[0]).pathname.split('/').slice(1).join('/')
                            }
                            assetsIds.push(assetObject);
                        }
                    }
                    var _matches2, regex2;
                    if (schema && schema.field_metadata && schema.field_metadata.markdown) {
                        regex2 = new RegExp('https://(dev-|stag-|)(assets|images).contentstack.io/v[\\d]/assets/(.*?)/(.*?)/(.*?)/download', 'g');
                    } else {
                        regex2 = new RegExp('https://(dev-|stag-|)(assets|images).contentstack.io/v[\\d]/assets/(.*?)/(.*?)/(.*?)/download', 'g');
                    }

                    while ((_matches2 = regex2.exec(_entry)) !== null) {
                        if (_matches2 && _matches2.length) {
                            var _assetObject = {};
                            if (_matches2[4]) _assetObject['uid'] = _matches2[4];
                            if (_matches2[0]) {
                                _assetObject['url'] = _matches2[0];
                                _assetObject['download_id'] = url.parse(_matches2[0]).pathname.split('/').slice(4).join('/');
                            }
                            assetsIds.push(_assetObject);
                        }
                    }
                    break;
            }
        };
        var get = function (parent, schema, entry) {
            var _entry = entry,
                len = parent.length;
            for (var j = 0; j < len; j++) {
                _entry = _entry[parent[j]];
                if (j == (len - 1) && _entry) {
                    if (_entry instanceof Array) {
                        for (var i = 0, _i = _entry.length; i < _i; i++) {
                            _get(schema, _entry[i]);
                        }
                    } else {
                        _get(schema, _entry);
                    }

                } else {
                    var _keys = _.cloneDeep(parent).splice(eval(j + 1), len);
                    if (_entry instanceof Array) {
                        for (var i = 0, _i = _entry.length; i < _i; i++) {
                            get(_keys, schema, _entry[i]);
                        }
                    } else if (typeof _entry != "object") {
                        break;
                    }
                }
            }
        };
        var find = function (schema, entry) {
            for (var i = 0, _i = schema.length; i < _i; i++) {
                if ((assetDownloadFlag && schema[i].data_type == "file") || (schema[i].data_type == "text")) {
                    parent.push(schema[i].uid);
                    get(parent, schema[i], entry);
                    parent.pop();
                }
                if (schema[i].data_type == "group") {
                    parent.push(schema[i].uid);
                    find(schema[i].schema, entry);
                    parent.pop();
                }
            }
        };
        find(data.content_type.schema, data.entry);
        return assetsIds;
    }
};

// Used to generate asset path from keys using asset
function urlFromObject(_asset) {
    try {
        var values = [],
            _keys = assetsConf.keys;
        for (var a = 0, _a = _keys.length; a < _a; a++) {
            if (_keys[a] === 'uid') {
                values.push((_asset._metadata && _asset._metadata.object_id) ? _asset._metadata.object_id : _asset.uid);
            } else if (_asset[_keys[a]]) {
                values.push(_asset[_keys[a]]);
            } else {
                throw new TypeError("'" + _keys[a] + "' key is undefined in asset object.");
            }
        }
        return values;
    } catch (error) {
        console.log(error);
    }
}


// Generate the full assets url foro the given url
function getAssetUrl (assetUrl, lang) {
    var relativeUrlPrefix = assetsConf.relative_url_prefix;
    assetUrl = relativeUrlPrefix + assetUrl;
    if (!(lang.relative_url_prefix == "/" || lang.host)) {
        assetUrl = lang.relative_url_prefix.slice(0, -1) + assetUrl;
    }
    return assetUrl;
}

/**
 * load assets from DB
 * @param  {Object} _assets - Contains path to each _asset.json depending on language in FS provider, empty for other's
 * @param  {String} lang    - Language from which asset's to be retrived
 * @return {Promise}        - On successful, returns an array containing all the assets of the specified language
 */

function loadAssets (lang) {
    return new Promise(function (resolve, reject) {
        try {
            db
                .Assets()
                .Query()
                .language(lang.code)
                .find()
                .spread(function (result) {
                    return resolve(JSON.parse(JSON.stringify(result)));
                }, function (error) {
                    return reject(error);
                })
        } catch (error) {
            return reject(error);
        }
    })
}


/**
 * Upsert published asset
 * @param  {Object}   _assetObject  : Asset object
 * @param  {Object}   _rteAsset     : The asset that's also referred in RTE
 * @param  {Object}   asset         : The asset object, that's been published
 * @param  {String}   _path         : Path to assets
 * @param  {Object}   lang          : Language object of the published asset
 * @param  {Function} callback      : Error-first callback
 * @return {Function}
 */

function upsertAsset (_assetObject, _rteAsset, asset, _path, lang, callback) {
    // console.log(" asset in upsert------------------ ",asset)
    var isForceLoad = asset.force_load || false;
    delete asset.ACL;
    delete asset.app_user_object_uid;
    delete asset.force_load;
    if (asset.publish_details) delete asset.publish_details;

    var paths = urlFromObject(asset),
        _url = getAssetUrl(paths.join('/'), lang);
    paths.unshift(_path);

    // current assets path
    var _assetPath = path.join.apply(path, paths);

    asset._internal_url = _url;
    if (!_.isEmpty(_assetObject) && _.isEqual(_assetObject[0], asset) && !isForceLoad && fs.existsSync(_assetPath)) {
        async.setImmediate(function () {
            return callback(null, asset);
        });
    } else {
        //remove old asset if not referred in RTE;
        if (asset && !_.isEmpty(_assetObject) && _.isEmpty(_rteAsset)) {
            var oldAssetPath = urlFromObject(_assetObject);
            // console.log(" oldAssetPath ------------- ",oldAssetPath)
            var old_uid = oldAssetPath[0];
            var old_filename = oldAssetPath[1];

            if(storageProvider == 'mongodb'){
                try {
                    // Remove old asset from s3 if not referred in RTE
                    var params = {Bucket: bucketName, Key:lang.code+"/"+old_uid+"/"+old_filename};
                    // console.log(" params for rte image --------------- ",params)
                    s3.headObject(params).on('success', function(response) {
                        s3.deleteObject(params, function (err, data) {
                            if (data) {
                                console.log("successfully remove old asset those not referred in RTE");
                            }
                            else {
                                console.log("Check if you have sufficient permissions : "+err);
                            }
                        });
                    }).on('error',function(error){
                        console.error('PK-S3 on error event', error);
                        // console.log(" error ------------------- ",error)
                        //error return a object with status code 404
                    }).send();
                    //    End to remove old asset from s3 if not referred in RTE
                } catch (e) {
                    console.error('417 PK-', e);
                }
            } else {
                try {
                    oldAssetPath.unshift(_path);
                    if (fs.existsSync(path.join.apply(path, oldAssetPath))) {
                        fs.unlinkSync(path.join.apply(path, oldAssetPath));
                    }
                } catch (e) {
                    console.error('426 PK-', e);
                }
            }
        }

        asset._internal_url = _url;
        helper.downloadAssets(_assetPath, asset,lang, function (error, data) {
            if (error) {
                async.setImmediate(function () {
                    return callback(error, null);
                });
            } else {
                db
                    .Assets(asset.uid)
                    .language(lang.code)
                    .update(asset)
                    .then(function () {
                        async.setImmediate(function () {
                            return callback(null, asset);
                        });
                    }, function (error) {
                        async.setImmediate(function () {
                            return callback(error, null);
                        });
                    });
            }
        });
    }
}

// download or remove assets
helper.getAssets = function () {
    return function (asset, lang, remove, cb) {
        // console.log(" asset in getAsets ------------------ ",asset)
        try {
            var _path = lang.assetsPath,
                assetUid = (!remove) ? asset.uid : asset,
                _assetObject = {}, _rteAsset = {};

            loadAssets(lang).then(function (result) {
                assets = result;
                if (assets && assets.length) {
                    _assetObject = _.find(assets, function (_asset) {
                        if (_asset.uid === assetUid && _asset._version) return _asset;
                    });
                    // check whether asset is referred in RTE/markdown
                    if (!_.isEmpty(_assetObject)) {
                        _rteAsset = _.find(assets, function (obj) {
                            if (obj.uid === _assetObject.uid && obj.download_id && obj.filename === _assetObject.filename) {
                                return obj;
                            }
                        });
                    }
                }

                // Publish/Unpublish
                if(!asset.download_id) {
                    if(remove) {
                        if (!_.isEmpty(_assetObject)) {
                            var _paths = urlFromObject(_assetObject);
                            _paths.unshift(_path);
                            var __assetPath = path.join.apply(path, _paths);
                            var isRemove = _.isEmpty(_rteAsset);
                            helper.unpublishAsset(asset, lang, __assetPath, isRemove, function (error, data) {
                                return cb(error, null)
                            });
                        } else {
                            async.setImmediate(function () {
                                return cb(null, null);
                            });
                        }
                    } else {
                        return upsertAsset(_assetObject, _rteAsset, asset, _path, lang, cb);
                    }
                } else {
                    // RTE/markdown assets download
                    var rteAssets = _.find(assets, {'download_id': asset.download_id, 'url': asset.url});
                    // console.log(" rteAssets ---------------- ",rteAssets)
                    if (rteAssets) {
                        async.setImmediate(function () {
                            return cb(null, rteAssets);
                        });
                    } else {
                        var paths = [assetUid];
                        paths.unshift(_path);
                        var assetPath = path.join.apply(path, paths);
                        helper.downloadAssets(assetPath, asset, lang, function (error, data) {
                            if(error) {
                                async.setImmediate(function () {
                                    return cb(error, null);
                                })
                            } else {
                                var paths = urlFromObject(data),
                                    _url = getAssetUrl(paths.join('/'), lang);
                                delete data._internal_url;
                                data._internal_url = _url;
                                db
                                    .Assets(data.download_id)
                                    .language(lang.code)
                                    .update(data)
                                    .then(function () {
                                        async.setImmediate(function () {
                                            return cb(null, data);
                                        });
                                    }, function (error) {
                                        async.setImmediate(function () {
                                            return cb(error, null);
                                        });
                                    });
                            }
                        });
                    }
                }
            });
        } catch (e) {
            async.setImmediate(function () {
                cb(e, null);
            });
        }
    };
}();

// download assets
helper.downloadAssets = function (assetsPath, asset, lang, callback) {
    headers.authtoken = headers.access_token;
    //starting to upload assets in s3
    if(storageProvider == 'mongodb'){
        var out = request({url: asset.url, headers: headers});
        var buffer = [];
        var i = 0
        out.on('response', function (resp) {
            if (resp.statusCode === 200) {
                // buffer.push(Buffer.from(resp));
                if (asset.download_id) {
                    var attachment = resp.headers['content-disposition'];
                    asset['filename'] = decodeURIComponent(attachment.split('=')[1]);
                }
            }
            resp.on('data', function(respData) {
                buffer.push(respData);
            })
        })
            .on('end', function() {
            var data = Buffer.concat(buffer);
            var params = {Bucket: bucketName, Key: lang.code+"/"+asset.uid+"/"+asset.filename, Body: data};
            // uploading data into s3 bucket
            s3.putObject(params, function (err, data) {
                if (err)
                    console.log("upload asset error in s3: ",err)
                else
                    console.log("Successfully uploaded assets in s3");
                callback(null, asset);
            });
        })
            .on('close', function () {
                callback(null, asset);
            });
        out.on('error', function (e) {
            callback("Error in media request: " + e.message, null);
        });
        out.end();
        //end to upload assets
    }
    else {
        var out = request({url: asset.url, headers: headers});
        out.on('response', function (resp) {
            if (resp.statusCode === 200) {
                if (asset.download_id) {
                    var attachment = resp.headers['content-disposition'];
                    asset['filename'] = decodeURIComponent(attachment.split('=')[1]);
                }
                var _path = assetsPath.replace(asset.filename, '');
                if (!fs.existsSync(_path)) helper.mkdirAllSync(_path, 0755);
                var localStream = fs.createWriteStream(path.join(_path, asset.filename));
                out.pipe(localStream);
                localStream.on('close', function () {
                    callback(null, asset);
                });
            } else {
                callback("No file found at given url: " + asset.url, null);
            }
        });
        out.on('error', function (e) {
            callback("Error in media request: " + e.message, null);
        });
        out.end();
    }
};

// unpublish Assets
helper.unpublishAsset = function (assetUid, lang, assetPath, isRemove, callback) {

    if(storageProvider == 'mongodb'){
        // Start to delete asset from s3 bucket
        var filename = assetPath.split('/')
        var file_index = filename.length - 1;
        filename = filename[file_index]
        var params = {Bucket: bucketName, Key: lang.code+"/"+assetUid+"/"+filename};

        s3.deleteObject(params, function (err, data) {
            if (data) {
                console.log("File deleted successfully");
            }
            else {
                console.log("Check if you have sufficient permissions : "+err);
            }
        });
        // End to delete asset from s3 bucket
    }
    else{
        if (isRemove && fs.existsSync(assetPath)) {
            fs.unlinkSync(assetPath);
        }
    }
    db
        .Assets(assetUid)
        .language(lang.code)
        .remove()
        .then(function () {
            return callback();
        }, function (err) {
            return callback(err);
        });
};


/**
 * Delete all assets which match the assetUid
 * @param  {String}   assetUid  : Uid of the asset/assets to be deleted
 * @param  {String}   lang      : Language that the asset exists in
 * @param  {Function} callback  : Error-first callback
 * @return {Function}
 */

helper.deleteAssets = function (assetUid, lang, callback) {
    try {
        var assetFolderPath = path.join(lang.assetsPath, assetUid);

        helper.deleteAssetFolder(assetFolderPath, assetUid, lang, function (error, data) {
            if (error) return callback(error, null);
            db
                .Assets()
                .language(lang.code)
                .Query()
                // _bulk_delete: ({_bulk_delete: true, objekts: {identifier: [Array of values to delete]}})
                // TODO: make the querying generic
                .query({_bulk_delete: true, objekts: {uid: [assetUid]}})
                .remove()
                .then(function (success) {
                    return callback(null, null);
                }, function (error) {
                    return callback(error, null);
                });
        });
    } catch (error) {
        return callback(error);
    }
};

// delete asset folder based on uid
helper.deleteAssetFolder = function (assetPath, assetUid, lang, callback) {
    // console.log("assetPath --------------------  ",assetPath)
    try {
        if(storageProvider == 'mongodb'){
            // Start to delete folder from S3
            var params = {
                Bucket: bucketName,
                Prefix: lang.code+'/'+assetUid
            };

            s3.listObjects(params, function(err, data) {
                if (err) return console.log(err);

                params = {Bucket: bucketName};
                params.Delete = {};
                params.Delete.Objects = [];

                data.Contents.forEach(function(content) {
                    params.Delete.Objects.push({Key: content.Key});
                });

                s3.deleteObjects(params, function(err, data) {
                    if (err) return console.log(err);

                    console.log("Number of object deleted from folder ---------- ",data.Deleted.length);
                    return callback(null, null);
                });
            });
            // End to delete folder from S3
        }
        else{
            if (fs.existsSync(assetPath)) {
                fs.readdir(assetPath, function (error, files) {
                    if(error)
                        return callback(error, null);

                    for (var i = 0, _i = files.length; i < _i; i++) {
                        fs.unlinkSync(path.join(assetPath, files[i]));
                    }
                    fs.rmdirSync(assetPath);
                    return callback(null, null);
                });
            } else {
                return callback(null, null)
            }
        }
    } catch (error) {
        return callback(error, null);
    }
};

// load plugins
helper.loadPlugins = function (dir) {
    var files = fs.readdirSync(dir);
    for (var i = 0, total = files.length; i < total; i++) {
        var pluginFolder = path.join(dir, files[i]);
        if (fs.lstatSync(pluginFolder).isDirectory()) {
            var plugin = path.join(pluginFolder, "index.js");
            if (fs.existsSync(plugin)) {
                require(plugin);
            }
        }
    }
};

// check value in string or array
helper.pluginChecker = function (str, value) {
    var flag = true;
    if (value && !((typeof value == "object" && value.indexOf(str) != -1) || value == str || value == "*")) {
        flag = false;
    }
    return flag;
};

// execute plugins
helper.executePlugins = function () {
    var plugins = utils.plugin._syncUtility,
        _environment = config.get('environment'),
        _server = config.get('server');
    return function (data, callback) {
        try {
            // load plugins
            // type, entry, contentType, lang, action
            var _loadPlugins = [],
                _data = {"language": data.language};

            switch (data.type) {
                case _types.entry:
                    _data.entry = data.entry;
                    _data.content_type = data.content_type;
                    break;
                case _types.asset:
                    _data.asset = data.asset;
                    break;
            }
            ;

            for (var i in plugins) {
                //if (helper.pluginChecker(contentType && contentType.uid, plugins[i].content_types) && helper.pluginChecker(lang.code, plugins[i].languages) && helper.pluginChecker(_environment, plugins[i].environments) && helper.pluginChecker(_server, plugins[i].servers) && plugins[i][action]) {
                if (plugins[i][data.action]) {
                    _loadPlugins.push(function (i) {
                        return function (cb) {
                            plugins[i][data.action](_data, cb);
                        };
                    }(i));
                }
            }
            async.series(_loadPlugins, function (error, res) {
                if (error)
                    return callback(error, null);

                switch (data.type) {
                    case _types.entry:
                        return callback(null, {"entry": data.entry, "content_type": data.content_type});
                        break;
                    case _types.asset:
                        return callback(null, {"asset": data.asset});
                        break;
                }
            });
        } catch (error) {
            return callback(error, null);
        }
    };
}();

// get message
helper.message = function (error) {
    if (_.isPlainObject(error)) {
        if (error.message) {
            return JSON.stringify(error.message);
        } else if (error.error_message) {
            return JSON.stringify(error.error_message);
        }
        return JSON.stringify(error);
    }
    return error;
};
