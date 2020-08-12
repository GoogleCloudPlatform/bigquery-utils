import {Progress, CancellationToken} from 'vscode';
import {exec} from 'child_process';
import {ResultJson} from './resultJson';

export class QueryBreakdownRunner {
    constructor(private execPath: string) {}

    execute(
        filepath: string, 
        progress: Progress<{message?: string | undefined;}>,
        token: CancellationToken): Promise<ResultJson[]> {
            if (token.isCancellationRequested) {
                return Promise.reject();
            }
            return new Promise<ResultJson[]>((resolve, reject) => {
                progress.report({message: 'Launching Query Breakdown...'});

                let jsonString = '';
                let errorMessage = '';
                const process = exec("java -jar " + this.execPath + " -i " + filepath + " -j").on('close', exitCode => {
                    if (!process.killed && exitCode === 0) {
                        resolve(JSON.parse(jsonString));
                    }
                    else {
                        reject();
                    }
                })
                .on('error', err => reject(err));

                process.stdout!.on('data', result => {
                    jsonString += result;
                });
                process.stderr!.on('data', message => {
                    errorMessage += message;
                });

                token.onCancellationRequested(() => process.kill());
            });
    }
}