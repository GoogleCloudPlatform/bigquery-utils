import * as path from 'path';
import * as vscode from 'vscode';
import {Highlighter} from './highlighter';
import {SqlExtractionRunner} from './sql_extraction_runner';
import {SqlExtractionProvider} from './tree_view';

// this method is called when your extension is activated
// your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
  const isWindows = process.platform === 'win32';
  const execPath = path.join(
    __filename,
    '..',
    '..',
    'resources',
    'sql_extraction',
    'bin',
    isWindows ? 'sql_extraction.bat' : 'sql_extraction'
  );

  const provider = new SqlExtractionProvider(
    new SqlExtractionRunner(execPath),
    new Highlighter(),
    vscode.workspace.rootPath
  );
  context.subscriptions.push(
    vscode.window.createTreeView('vscode-sql-extraction.tree-view', {
      treeDataProvider: provider,
    })
  );
  context.subscriptions.push(
    vscode.commands.registerCommand('vscode-sql-extraction.run', async () => {
      vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Finding all queries',
          cancellable: true,
        },
        async (progress, token) => {
          return await provider.refresh(progress, token);
        }
      );
    })
  );
  context.subscriptions.push(
    vscode.commands.registerCommand('vscode-sql-extraction.onclick', sqlQuery =>
      sqlQuery.onClick()
    )
  );

  vscode.window.onDidChangeActiveTextEditor(
    editor => provider.highlight(editor),
    null,
    context.subscriptions
  );
}

// this method is called when your extension is deactivated
export function deactivate() {}
