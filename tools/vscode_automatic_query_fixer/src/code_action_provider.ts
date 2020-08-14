import * as vscode from 'vscode';
import {QueryFix} from './auto_fixer_result';

/**
 * Provides error highlights and code fixes for the currently open document.
 */
export class AutoFixerActionProvider implements vscode.CodeActionProvider {
  private fixes?: QueryFix[];
  private uri?: vscode.Uri;
  /** Location of scanned query. Currently, it's the entire document. */
  private range?: vscode.Range;

  constructor(
    private readonly diagnosticCollection: vscode.DiagnosticCollection
  ) {}

  /**
   * Initializes the errors to show on [openEditor] and any potential fixes.
   */
  setFixes(fixes: QueryFix[], openEditor: vscode.TextEditor) {
    let hasErrors = false;

    const diagnostics: vscode.Diagnostic[] = [];
    fixes.forEach(fix => {
      if (!fix.error || !fix.errorPosition) {
        return;
      }
      hasErrors = true;

      let msg = fix.error!;
      if (fix.approach) {
        msg += ' ' + fix.approach!;
      }

      // highlight the immediate token
      const range = openEditor.document.getWordRangeAtPosition(
        new vscode.Position(
          fix.errorPosition.row - 1,
          fix.errorPosition.column - 1
        )
      )!;

      diagnostics.push(
        new vscode.Diagnostic(range, msg, vscode.DiagnosticSeverity.Error)
      );
    });

    if (!hasErrors) {
      vscode.window.showInformationMessage('No errors were found.');
    }

    this.diagnosticCollection.set(openEditor.document.uri, diagnostics);

    this.fixes = fixes;
    this.uri = openEditor.document.uri;
    // set range to entire document text
    this.range = openEditor.document.validateRange(
      new vscode.Range(0, 0, openEditor.document.lineCount, 0)
    );
  }

  /**
   * Clears all error highlights.
   */
  clear() {
    this.fixes = undefined;
    this.uri = undefined;
    this.range = undefined;
    this.diagnosticCollection.clear();
  }

  /**
   * Called by the API to potentially provide code fixes for the given [document] and [range].
   */
  provideCodeActions(
    document: vscode.TextDocument,
    range: vscode.Range | vscode.Selection
  ): vscode.ProviderResult<(vscode.Command | vscode.CodeAction)[]> {
    if (!this.fixes || document.uri !== this.uri) {
      return [];
    }

    return this.fixes
      .filter(
        fix =>
          // for all errors contained in the given range with code fixes
          fix.options &&
          fix.errorPosition &&
          range.contains(
            new vscode.Position(
              fix.errorPosition.row - 1,
              fix.errorPosition.column - 1
            )
          )
      )
      .map(fix =>
        // create an action to replace the query with the fix
        fix.options!.map(option => {
          const workspaceEdit = new vscode.WorkspaceEdit();
          workspaceEdit.replace(this.uri!, this.range!, option.fixedQuery);
          const action = new vscode.CodeAction(
            `${option.description}. Replace query with ${option.fixedQuery}`,
            vscode.CodeActionKind.QuickFix
          );
          action.diagnostics = this.diagnosticCollection
            .get(this.uri!)!
            .slice();
          action.edit = workspaceEdit;
          return action;
        })
      )
      .reduce((acc, val) => acc.concat(val)); // flatten map
  }
}
