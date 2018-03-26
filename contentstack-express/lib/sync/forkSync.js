var sync = require('./index');

// Export sync, so as to run on a separate thread
module.exports = new sync(true);