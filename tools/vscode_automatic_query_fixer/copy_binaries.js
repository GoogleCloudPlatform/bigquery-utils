const {spawn} = require('child_process');
const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf');

const projectDir = '../automatic_query_fixer/';
const destinationDir = './resources/automatic_query_fixer';

// launch gradle build
let jarBuild;
if (process.platform === 'win32') {
  const batchScript = path.resolve(path.join(projectDir, 'gradlew.bat'));
  jarBuild = spawn(batchScript, ['installDist'], {
    cwd: projectDir,
  });
} else {
  jarBuild = spawn('sh', ['gradlew', 'installDist'], {
    cwd: projectDir,
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
      path.join(projectDir, 'build/install/AutomaticQueryFixer'),
      destinationDir
    );
    console.log('\nAuto Fixer binaries updated successfully!');
  }
});
