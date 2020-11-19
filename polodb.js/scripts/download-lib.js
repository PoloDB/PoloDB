
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const version = '0.3.0';

const platform = os.platform();
const arch = os.arch();
const downloadUrl = `https://www.polodb.org/resources/${version}/node/${platform}/${arch}/polodb-js.node`;
const downloadChecksumUrl = `${downloadUrl}.SHA256`;

function getDownloadPath() {
  const tmpDir = os.tmpdir();
  const projectDir = path.join(tmpDir, 'polodb-node');
  if (!fs.existsSync(projectDir)) {
    fs.mkdirSync(projectDir);
  }
  const nodeFilePath = path.join(projectDir, 'polodb-js.node');
  return nodeFilePath;
}

function downloadChecksumFile() {
  return new Promise((resolve, reject) => {
    const checksumPath = getDownloadPath() + '.SHA256';
    const file = fs.createWriteStream(checksumPath);
    https.get(downloadChecksumUrl, function(resp) {
      resp.pipe(file);
      resp.on('error', err => {
        reject(err);
      });
      resp.on('end', () => {
        if (resp.complete) {
          resolve(checksumPath);
        }
      });
    });

  });
}

function downloadLib(url, path) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(path);
    https.get(url, function (resp) {
      resp.pipe(file);
      resp.on('error', err => {
        reject(err);
      });
      resp.on('end', () => {
        if (resp.complete) {
          resolve();
        }
      })
    });
  });
}

function calsha256(filename) {
  return new Promise((resolve, reject) => {
    const sum = crypto.createHash('sha256');
    const fileStream = fs.createReadStream(filename);
    fileStream.on('error', function (err) {
      return reject(err)
    });
    fileStream.on('data', function (chunk) {
      try {
        sum.update(chunk);
      } catch (ex) {
        return reject(ex);
      }
    });
    fileStream.on('end', function () {
      return resolve(sum.digest('hex'))
    });
  });
};

async function checksum(checksumFilePath, libPath) {
  let checksumContent = fs.readFileSync(checksumFilePath, 'utf-8');
  let actualChecksum = await calsha256(libPath);
  return checksumContent === actualChecksum;
}

async function main() {
  try {
    const checksumPath = await downloadChecksumFile();

    const nodeFilePath = getDownloadPath();
    console.log('PoloDB lib path: ', nodeFilePath);

    if (!fs.existsSync(nodeFilePath)) {
      console.log('lib not found, begin to download from: ', downloadUrl);
      await downloadLib(downloadUrl, nodeFilePath);
    }

    if (!await checksum(checksumPath, nodeFilePath)) {
      console.log('checksum mismatch');
      process.exit(-1);
    }

    copyNodeToDest(nodeFilePath);
  } catch (err) {
    console.error(err);
    process.exit(-1);
  }
}

function copyNodeToDest(nodeFilePath) {
  fs.copyFileSync(nodeFilePath, path.join(__dirname, '..', 'polodb-js.node'));
}

main();
