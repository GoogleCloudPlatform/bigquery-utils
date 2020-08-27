import {Progress, CancellationToken} from 'vscode';
import {execFile} from 'child_process';
import {QueryFix} from './auto_fixer_result';

/**
 * Runs an executable file provided by the Auto Fixer distribution.
 */
export class AutoFixerRunner {
  constructor(private execPath: string) {}

  /**
   * execute Auto Fixer to analyze [query].
   */
  public analyze(
    query: string,
    progress: Progress<{
      message?: string | undefined;
      increment?: number | undefined;
    }>,
    token: CancellationToken
  ): Promise<QueryFix[]> {
    return this.execute(
      ['-m', 'fo', '-o', 'json', '-p', 'sql-gravity-internship', query],
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
  ): Promise<QueryFix[]> {
    if (token.isCancellationRequested) {
      return Promise.reject();
    }

    return new Promise<QueryFix[]>((resolve, reject) => {
      progress.report({message: 'Launching Automatic Query Fixer...'});

      let json = '';
      let errMsg = '';
      let totalProgress = 0;
      const process = execFile(this.execPath, args)
        .on('close', code => {
          if (!process.killed && code === 0) {
            let fixes = JSON.parse(json);
            if (!Array.isArray(fixes)) {
              // treat all output payload as an array
              fixes = [fixes];
            }
            resolve(fixes);
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
