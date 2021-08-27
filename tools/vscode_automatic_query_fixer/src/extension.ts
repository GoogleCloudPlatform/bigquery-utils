import * as vscode from 'vscode';
import * as path from 'path';
import {AutoFixerRunner} from './auto_fixer_runner';
import {AutoFixerActionProvider} from './code_action_provider';

export function activate(context: vscode.ExtensionContext) {
  const isWindows = process.platform === 'win32';
  const execPath = path.join(
    __filename,
    '..',
    '..',
    'resources',
    'automatic_query_fixer',
    'bin',
    isWindows ? 'AutomaticQueryFixer.bat' : 'AutomaticQueryFixer'
  );

  const diagnosticCollection = vscode.languages.createDiagnosticCollection(
    'autofix'
  );
  context.subscriptions.push(diagnosticCollection);
  const codeActionProvider = new AutoFixerActionProvider(diagnosticCollection);
  context.subscriptions.push(
    vscode.languages.registerCodeActionsProvider(
      [{scheme: 'file'}, {scheme: 'untitled'}],
      codeActionProvider
    )
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'vscode-automatic-query-fixer.runAutoFixer',
      async () => {
        vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: 'Analyzing query',
            cancellable: true,
          },
          async (progress, token) => {
            try {
              const openEditor = vscode.window.activeTextEditor;
              if (!openEditor) {
                return;
              }

              const fixes = await new AutoFixerRunner(execPath).analyze(
                openEditor.document.getText(),
                progress,
                token
              );

              codeActionProvider.setFixes(fixes, openEditor);
            } catch (error) {
              vscode.window.showErrorMessage(error);
              throw error;
            }
          }
        );
      }
    )
  );

  vscode.workspace.onDidChangeTextDocument(
    () => {
      diagnosticCollection.clear();
      codeActionProvider.clear();
    },
    null,
    context.subscriptions
  );
}
