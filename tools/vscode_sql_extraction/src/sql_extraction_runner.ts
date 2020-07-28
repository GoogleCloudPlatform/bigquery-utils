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
    return this.execute(['-r', '--progress', dir], progress, token);
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
      let json = '';
      let errMsg = '';
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
        let index = errMsg.indexOf('\n');
        while (index >= 0) {
          // one entire error message was received completely up to newline
          const statement = errMsg.substring(0, index);
          errMsg = errMsg.substring(index + 1);
          index = errMsg.indexOf('\n');

          // if the error message starts with a percentage
          if (statement.match(/^\d+(\.\d*)?% .*$/)) {
            const percent = parseFloat(statement);
            const message = statement.substring(statement.indexOf('%' + 2));
            progress.report({
              message: message,
              increment: isNaN(percent) ? undefined : percent,
            });
          }
        }
      });
      token.onCancellationRequested(() => process.kill());
    });
  }
}
