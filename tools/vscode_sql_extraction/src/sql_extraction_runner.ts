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
    return this.execute(['-r', dir], progress, token);
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
      // todo: track progress
      token.onCancellationRequested(() => process.kill());
    });
  }
}
