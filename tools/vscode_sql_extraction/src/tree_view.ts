import * as path from 'path';
import * as vscode from 'vscode';
import {Highlighter} from './highlighter';
import {SqlExtractionRunner} from './sql_extraction_runner';
import {Query, QueryFragment, locationToRange, toCombinedString} from './query';

/**
 * Provides ordering in the tree panel.
 */
export class SqlExtractionProvider
  implements vscode.TreeDataProvider<SqlQueryItem> {
  private queries?: Query[];

  private _onDidChangeTreeData: vscode.EventEmitter<
    SqlQueryItem | undefined
  > = new vscode.EventEmitter<SqlQueryItem | undefined>();
  readonly onDidChangeTreeData: vscode.Event<SqlQueryItem | undefined> = this
    ._onDidChangeTreeData.event;

  constructor(
    private readonly sqlExtractor: SqlExtractionRunner,
    private readonly highlighter: Highlighter,
    private readonly workspaceRoot?: string
  ) {}

  /**
   * Runs SQL Extraction to analyze all files.
   *
   * @param progress progress bar
   * @param token cancellation token
   */
  async refresh(
    progress: vscode.Progress<{
      message?: string | undefined;
      increment?: number | undefined;
    }>,
    token: vscode.CancellationToken
  ): Promise<void> {
    if (this.workspaceRoot) {
      if (
        vscode.workspace.textDocuments.filter(i => i.isDirty || i.isUntitled)
          .length > 0
      ) {
        vscode.window.showInformationMessage(
          'Unsaved changes found. These will not show up on analysis.'
        );
      }

      try {
        this.queries = await this.sqlExtractor.extractFromDirectory(
          this.workspaceRoot,
          progress,
          token
        );
      } catch (error) {
        return;
      }
    }

    this._onDidChangeTreeData.fire(undefined);

    const openEditor = vscode.window.activeTextEditor;
    this.highlight(openEditor);
  }

  getTreeItem(element: SqlQueryItem): vscode.TreeItem {
    return element;
  }

  getChildren(element?: SqlQueryItem): Thenable<SqlQueryItem[]> {
    if (!this.workspaceRoot) {
      vscode.window.showInformationMessage(
        'Cannot use SQL Extension in an empty workspace.'
      );
      return Promise.resolve([]);
    }

    if (!this.queries) {
      // Didn't run SQL Extraction yet
      return Promise.resolve([]);
    }

    if (!element) {
      // root elements
      return Promise.resolve(
        this.queries.map(query => {
          if (!query.query.treeItem) {
            query.query.treeItem = new SqlQueryItem(query, query.query);
          }
          return query.query.treeItem;
        })
      );
    }

    const parent = element.queryFragment;
    if (parent) {
      if (parent.literal) {
        return Promise.resolve([]);
      }

      return Promise.resolve(
        parent.complex!.map(query => {
          if (!query.treeItem) {
            query.treeItem = new SqlQueryItem(element.parentQuery, query);
          }
          return query.treeItem;
        })
      );
    }

    // todo: show usages
    return Promise.resolve([]);
  }

  highlight(openEditor?: vscode.TextEditor) {
    if (!openEditor || !this.workspaceRoot || !this.queries) {
      return;
    }

    this.highlighter.highlight(openEditor, this.workspaceRoot!, this.queries!);
  }
}

/**
 * Individual clickable item populating the tree view.
 * todo: show usages.
 */
export class SqlQueryItem extends vscode.TreeItem {
  constructor(
    public readonly parentQuery: Query,
    public readonly queryFragment?: QueryFragment
  ) {
    super(
      queryFragment
        ? toCombinedString(queryFragment).replace(/\s+/g, ' ')
        : 'Usage',
      queryFragment?.literal
        ? vscode.TreeItemCollapsibleState.None
        : vscode.TreeItemCollapsibleState.Collapsed
    );
    super.command = {
      command: 'vscode-sql-extraction.onclick',
      title: '',
      arguments: [this],
    };
  }

  get tooltip(): string {
    return this.parentQuery.confidence.toString() ?? '';
  }

  get description(): string {
    return this.parentQuery.file;
  }

  iconPath = {
    light: path.join(
      __filename,
      '..',
      '..',
      'resources',
      'light',
      'dependency.svg'
    ),
    dark: path.join(
      __filename,
      '..',
      '..',
      'resources',
      'dark',
      'dependency.svg'
    ),
  };

  contextValue = 'sqlquery';

  /**
   * Called when this item is clicked by the user.
   * Open and select the relevant file location.
   */
  onClick() {
    let range: vscode.Range | undefined = undefined;
    if (this.queryFragment) {
      range = locationToRange(this.queryFragment.location);
    }

    let filePath = this.parentQuery.file;
    if (!path.isAbsolute(filePath)) {
      filePath = path.join(vscode.workspace.rootPath!, this.parentQuery.file);
    }

    vscode.window.showTextDocument(vscode.Uri.file(filePath), {
      selection: range,
    });
  }
}
