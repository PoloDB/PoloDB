const addon = require('bindings')('polodb-js');

function version() {
  return addon.version();
}

module.exports = {
  Database: addon.Database,
  version,
};
