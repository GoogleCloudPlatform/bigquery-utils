import * as vscode from 'vscode';

const decorationTypeParseable = vscode.window.createTextEditorDecorationType({
  backgroundColor: '#21bf2b',
});

const decorationTypeUnparseable = vscode.window.createTextEditorDecorationType({
  backgroundColor: '#8f1713',
});

const json = [
  {
    error_position: {startLine: 1, startColumn: 1, endLine: 1, endColumn: 4	},
    error_type: 'DELETION',
    replacedFrom: null,
    replacedTo: null,
  },
  {
    error_position: {startLine: 2, startColumn: 1, endLine: 2, endColumn: 4},
    error_type: 'DELETION',
    replacedFrom: null,
    replacedTo: null,
  },
  {
    error_position: {startLine: 2, startColumn: 28, endLine: 2, endColumn: 31},
    error_type: 'REPLACEMENT',
    replacedFrom: 'BLAH',
    replacedTo: 'BY',
  },
];

// this method is called when the extension is activated
export function activate(context: vscode.ExtensionContext) {
  // The command has been defined in the package.json file
  const disposable = vscode.commands.registerCommand(
    'vscode-query-breakdown.run',
    () => {
      // Display a message box to the user
      vscode.window.showInformationMessage(
        'vscode_query_breakdown is running!'
      );
      const currentEditor = vscode.window.activeTextEditor;
      if (!currentEditor) {
        vscode.window.showInformationMessage(
          'there is no editor open currently'
        );
        return;
      }

      // highlights and creates hovers for queries
      decorate(currentEditor);
    }
  );

  context.subscriptions.push(disposable);
}

function decorate(editor: vscode.TextEditor) {
  const decorationUnparseableArray: vscode.DecorationOptions[] = [];
  const decorationParseableArray: vscode.DecorationOptions[] = [];

  // parses through the json objects
  for (let i = 0; i < json.length; i++) {
    // finds error position
    const errorRange = new vscode.Range(
      json[i].error_position.startLine - 1,
      json[i].error_position.startColumn - 1,
      json[i].error_position.endLine - 1,
	  json[i].error_position.endColumn
	);
	console.log(json[i].error_position.endColumn)
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
}

// this method is called when your extension is deactivated
export function deactivate() {}
