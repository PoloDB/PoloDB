let addon;

if (process.env['PLDB_LOCAL'] === 'true') {
  addon = require('bindings')('polodb-js');
} else {
  addon = require('./polodb-js');
}

function version() {
  return addon.version();
}

module.exports = {
  Database: addon.Database,
  version,
};
