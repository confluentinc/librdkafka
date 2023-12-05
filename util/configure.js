'use strict';

var query = process.argv[2];

var fs = require('fs');
var path = require('path');

var baseDir = path.resolve(__dirname, '../');
var releaseDir = path.join(baseDir, 'build', 'deps');
// var command = './configure --install-deps --source-deps-only --disable-lz4-ext --enable-static --enable-strip --disable-gssapi';
// if (!process.env.IS_ON_CI) {
var command = './configure --install-deps --source-deps-only --disable-lz4-ext --enable-static --enable-strip --disable-gssapi --prefix=' + releaseDir + ' --libdir=' + releaseDir;
// }

var isWin = /^win/.test(process.platform);

// Skip running this if we are running on a windows system
if (isWin) {
  process.stderr.write('Skipping run because we are on windows\n');
  process.exit(0);
}

var childProcess = require('child_process');

try {
  process.stderr.write("Running: " + command + 'on working directory = ' + baseDir + '\n');
  childProcess.execSync(command, {
    cwd: baseDir,
    stdio: [0,1,2]
  });
  process.exit(0);
} catch (e) {
  process.stderr.write(e.message + '\n');
  process.exit(1);
}
