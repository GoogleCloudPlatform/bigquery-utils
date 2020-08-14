import {Progress, CancellationToken} from 'vscode';
import {execFile} from 'child_process';
import {Query} from './query';

/**
 * Runs an executable file provided by the SQL Extraction distribution.
 */
export class SqlExtractionRunner {
  constructor(private execPath: string) {}

  /**
   * Runs the executable and returns the results.
   *
   * @param dir Directory (recursive) to run SQL extraction at.
   * @param progress Progress bar.
   * @param token CancellationToken.
   * @returns List of Json objects (each representing a complete query).
   */
  extractFromDirectory(
    dir: string,
    progress: Progress<{
      message?: string | undefined;
      increment?: number | undefined;
    }>,
    token: CancellationToken
  ): Promise<Query[]> {
    return this.execute(
      ['-r', '--progress', '--parallel', dir],
      progress,
      token
    );
  }

  private execute(
    args: string[],
    progress: Progress<{
      message?: string | undefined;
      increment?: number | undefined;
    }>,
    token: CancellationToken
  ): Promise<Query[]> {
    if (token.isCancellationRequested) {
      return Promise.reject();
    }

    return new Promise<Query[]>((resolve, reject) => {
      progress.report({message: 'Launching SQL Extraction...'});

      let json = '';
      let errMsg = '';
      let totalProgress = 0;
      const process = execFile(this.execPath, args)
        .on('close', code => {
          if (!process.killed && code === 0) {
            resolve(JSON.parse(json).queries);
          } else {
            reject();
          }
        })
        .on('error', err => reject(err));

      process.stdout!.on('data', data => {
        json += data;
      });
      process.stderr!.on('data', data => {
        errMsg += data;
        const lines = errMsg.split('\n');
        // if at least one entire message was received completely up to newline
        if (lines.length > 1) {
          for (let i = 0; i < lines.length - 2; i++) {
            const statement = lines[i];

            // if the error message starts with a percentage
            if (statement.match(/^\d+(\.\d*)?% .*$/)) {
              const percent = parseFloat(statement);
              const nextTotalProgress = isNaN(percent)
                ? totalProgress
                : percent;
              const message = statement.substring(statement.indexOf('%' + 2));
              progress.report({
                message: message,
                increment: nextTotalProgress - totalProgress,
              });
              totalProgress = nextTotalProgress;
            }
          }
          // save the remainder for later
          // this works even if '\n' appears at the very end
          errMsg = lines[lines.length - 1];
        }
      });
      token.onCancellationRequested(() => process.kill());
    });
  }
}
