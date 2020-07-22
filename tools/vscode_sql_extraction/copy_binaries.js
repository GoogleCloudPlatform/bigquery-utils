const {spawn} = require('child_process');
const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf');

const sqlExtractionProjectDir = '../sql_extraction/';
const destinationDir = './resources/sql_extraction';

// launch gradle build
let jarBuild;
if (process.platform === 'win32') {
  const batchScript = path.resolve(
    path.join(sqlExtractionProjectDir, 'gradlew.bat')
  );
  jarBuild = spawn(batchScript, ['installDist'], {
    cwd: sqlExtractionProjectDir,
  });
} else {
  jarBuild = spawn('sh', ['gradlew', 'installDist'], {
    cwd: sqlExtractionProjectDir,
  });
}
jarBuild.stdout.pipe(process.stdout);
jarBuild.stderr.pipe(process.stderr);
jarBuild.on('close', code => {
  if (code === 0) {
    // delete previous copy
    if (fs.existsSync(destinationDir)) {
      rimraf.sync(destinationDir);
    }
    // move binaries to resources folder
    fs.renameSync(
      path.join(sqlExtractionProjectDir, 'build/install/sql_extraction'),
      destinationDir
    );
    console.log('\nSQL Extraction binaries updated successfully!');
  }
});
