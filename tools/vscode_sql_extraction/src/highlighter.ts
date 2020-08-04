import * as path from 'path';
import * as vscode from 'vscode';
import {Query, locationToRange} from './query';
import randomColor from 'randomcolor';

/**
 * Highlights found queries in the text editor.
 */
export class Highlighter {
  decorations: vscode.TextEditorDecorationType[] = [];

  /**
   * Highlights all queries in the open document.
   *
   * @param openEditor currently open editor.
   * @param workspaceRoot root path of the open workspace.
   * @param queries list of found queries.
   */
  highlight(
    openEditor: vscode.TextEditor,
    workspaceRoot: string,
    queries: Query[]
  ) {
    const currentQueries = this.getAllQueriesInCurrentFile(
      openEditor,
      workspaceRoot,
      queries
    );
    if (currentQueries.length <= 0) {
      return;
    }

    currentQueries.forEach((query, index) => {
      const ranges: vscode.Range[] = [];

      const workRemaining = [query.query];
      while (workRemaining.length > 0) {
        const fragment = workRemaining.pop()!;
        if (fragment.literal) {
          ranges.push(locationToRange(fragment.location));
        } else {
          // recurse for child fragments
          fragment.complex!.forEach(child => {
            workRemaining.push(child);
          });
        }
      }

      query.usages.forEach(usage => {
        ranges.push(locationToRange(usage));
      });

      openEditor.setDecorations(this.getColor(index), ranges);
    });
  }

  /**
   * Gets all queries located in the currently open file.
   *
   * @param openEditor currently open editor.
   * @param workspaceRoot root path of the open workspace.
   * @param queries list of found queries.
   */
  private getAllQueriesInCurrentFile(
    openEditor: vscode.TextEditor,
    workspaceRoot: string,
    queries: Query[]
  ) {
    let openPath = openEditor.document.uri.path.toString();
    if (!path.isAbsolute(openPath)) {
      openPath = path.join(workspaceRoot, openPath);
    }

    return queries.filter(query => {
      let filePath = query.file;
      if (!path.isAbsolute(filePath)) {
        filePath = path.join(workspaceRoot!, query.file);
      }
      return filePath === openPath;
    });
  }

  /**
   * Updates the cache if needed, and then returns the relevant decoration.
   *
   * @param index decoration number to return.
   */
  private getColor(index: number): vscode.TextEditorDecorationType {
    while (this.decorations.length <= index) {
      // get a random light color with fixed opacity
      // todo: use highlight color of the current theme
      const color = randomColor({luminosity: 'light', hue: 'random'}) + '29';
      const decoration = vscode.window.createTextEditorDecorationType({
        backgroundColor: color,
      });
      this.decorations.push(decoration);
    }

    return this.decorations[index];
  }
}
