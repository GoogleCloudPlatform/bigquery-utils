import {Progress, CancellationToken} from 'vscode';
import {execFile} from 'child_process';

export class QueryBreakdownRunner {
    constructor(private execPath: string) {}

    execute(args: string[], progress: Progress<message>) {

    }
}