import {Progress, CancellationToken} from 'vscode';
import {exec} from 'child_process';
import {ResultJson} from './resultJson';
import * as config from './toolconfig.json';

/**
 * query_breakdown_runner contains the QueryBreakdownRunner class that is used to
 * run the backend (query_breakdown) jar file from VS code and extract the results.
 * It contains the execute function which returns a list of JSON objects that represent
 * the unparseable components.
 */
export class QueryBreakdownRunner {
  constructor(private execPath: string) {}

  // method that executes the query_breakdown program. It returns a promise as it is called asynchronously
  execute(
    filepath: string,
    progress: Progress<{message?: string | undefined}>,
    token: CancellationToken
  ): Promise<ResultJson[]> {
    if (token.isCancellationRequested) {
      return Promise.reject();
    }
    return new Promise<ResultJson[]>((resolve, reject) => {
      progress.report({message: 'Launching Query Breakdown...'});

      let jsonString = '';
      let errorMessage = '';
      let command = 'java -jar ' + this.execPath + ' -i ' + filepath + ' -j';
      if (config.runtimeLimit) {
        command = command + ' -l ' + config.runtimeLimit;
      }
      if (config.replacementLimit) {
        command = command + ' -r ' + config.replacementLimit;
      }
      // executes the jar file
      const process = exec(command)
        .on('close', exitCode => {
          if (!process.killed && exitCode === 0) {
            // returns the array of json objects after parsing the result
            resolve(JSON.parse(jsonString));
          } else {
            reject(errorMessage);
          }
        })
        .on('error', err => reject(err));

      // adds the standard output from backend to the jsonString variable for parsing
      process.stdout!.on('data', result => {
        jsonString += result;
      });
      // extracts the error message if there is any
      process.stderr!.on('data', message => {
        errorMessage += message;
      });

      token.onCancellationRequested(() => process.kill());
    });
  }
}
