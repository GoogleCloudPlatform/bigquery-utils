import * as vscode from 'vscode';
import * as path from 'path';
import {ResultJson} from './resultJson';
import {QueryBreakdownRunner} from './query_breakdown_runner';

/**
 * extension.ts contains the main code for the extension. It deals with the logic of
 * what happens when the extension is activated
 */

// colors for the highlight
const decorationTypeParseable = vscode.window.createTextEditorDecorationType({
  backgroundColor: '#137333',
});

const decorationTypeUnparseable = vscode.window.createTextEditorDecorationType({
  backgroundColor: '#4285F4',
});

// variable to keep track of results of backend
let json: ResultJson[];

// this method is called when the extension is activated
export function activate(context: vscode.ExtensionContext) {
  const execPath = path.join(
    __filename,
    '..',
    '..',
    'resources',
    'query_breakdown',
    'bin',
    'query_breakdown.jar'
  );
  // The command has been defined in the package.json file
  const disposable = vscode.commands.registerCommand(
    'vscode-query-breakdown.run',
    async () => {
      // Display a message box to the user
      vscode.window.showInformationMessage(
        'vscode_query_breakdown is running!'
      );

      // Display that progress is being made (the tool is running)
      vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Finding Unparsable Components',
          cancellable: true,
        },
        async (progress, token) => {
          const runner = new QueryBreakdownRunner(execPath);
          const currentEditor = vscode.window.activeTextEditor;
          if (currentEditor) {
            // get results from the backend
            json = await runner.execute(
              currentEditor.document.uri.fsPath,
              progress,
              token
            );
            if (!json) {
              vscode.window.showInformationMessage(
                'There was an error in fetching results from the backend'
              );
            } else if (json.length === 0) {
              vscode.window.showInformationMessage(
                'The entire query can be parsed without error'
              );
            } else {
              // highlights and creates hovers for queries
              decorate(currentEditor);
            }
          } else {
            vscode.window.showInformationMessage(
              'there is no editor open currently'
            );
          }
        }
      );
    }
  );

  context.subscriptions.push(disposable);
}

function decorate(editor: vscode.TextEditor) {
  const decorationUnparseableArray: vscode.DecorationOptions[] = [];
  const decorationParseableArray: vscode.DecorationOptions[] = [];

  // parses through the json objects
  for (let i = 0; i < json.length - 2; i++) {
    // finds error position
    const errorRange = new vscode.Range(
      json[i].error_position.startLine - 1,
      json[i].error_position.startColumn - 1,
      json[i].error_position.endLine - 1,
      json[i].error_position.endColumn
    );
    // deletion case
    if (json[i].error_type === 'DELETION') {
      const deletionMessage = new vscode.MarkdownString('Deleted');
      decorationUnparseableArray.push({
        range: errorRange,
        hoverMessage: deletionMessage,
      });
    }
    // replacement case
    else if (json[i].error_type === 'REPLACEMENT') {
      const replacementMessage = new vscode.MarkdownString(
        'Replaced ' + json[i].replacedFrom + ' with ' + json[i].replacedTo
      );
      decorationUnparseableArray.push({
        range: errorRange,
        hoverMessage: replacementMessage,
      });
    } else {
      // error handling
      continue;
    }
  }

  // constructs decoration option for entire document
  const entireDocument = new vscode.Range(
    editor.document.lineAt(0).range.start,
    editor.document.lineAt(editor.document.lineCount - 1).range.end
  );
  decorationParseableArray.push({range: entireDocument});

  // sets the decorations
  editor.setDecorations(decorationTypeParseable, decorationParseableArray);
  editor.setDecorations(decorationTypeUnparseable, decorationUnparseableArray);

  vscode.window.showInformationMessage(
    'Percentage of Parseable Components: ' +
      json[json.length - 2].performance +
      '%'
  );
  vscode.window.showInformationMessage(
    'Runtime: ' + json[json.length - 1].runtime + ' seconds'
  );
}
