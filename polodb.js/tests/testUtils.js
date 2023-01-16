const path = require('path');
const os = require('os');
const fs = require('fs');

/**
 * @param {string} name 
 */
function prepareTestPath(name) {
  const p = path.join(os.tmpdir(), name);
  const journalPath = p + '.journal';
  try {
    fs.unlinkSync(p);
    fs.unlinkSync(journalPath);
  } catch (err) {}
  return p;
}

module.exports = {
  prepareTestPath,
};
