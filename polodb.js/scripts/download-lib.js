
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');

const version = '0.1.0';

const platform = os.platform();
const arch = os.arch();
const downloadUrl = `https://www.polodb.org/resources/${version}/node/${platform}/${arch}/polodb-js.node`;

function getDownloadPath() {
  const tmpDir = os.tmpdir();
  const projectDir = path.join(tmpDir, 'polodb-node');
  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir);
  }
  const nodeFilePath = path.join(projectDir, 'polodb-js.node');
  return nodeFilePath;
}

const nodeFilePath = getDownloadPath();
console.log('PoloDB lib path: ', nodeFilePath);
if (!fs.existsSync(nodeFilePath)) {
  const file = fs.createWriteStream(nodeFilePath);
  https.get(downloadUrl, function(response) {
    response.pipe(file);
    response.on('close', () => {
      console.log('download finished');
      copyNodeToDest();
    });
  });
} else {
  copyNodeToDest();
}

function copyNodeToDest() {
  fs.copyFileSync(nodeFilePath, path.join(__dirname, '..', 'polodb-js.node'));
}
