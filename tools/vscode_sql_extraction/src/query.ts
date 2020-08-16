import {SqlQueryItem} from './tree_view';
import {Range} from 'vscode';

export interface Query {
  file: string;
  confidence: number;
  query: QueryFragment;
  usages: Location[];
  notes: string;
}

export interface Location {
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

export function locationToRange(location: Location): Range {
  return new Range(
    location.startLine - 1,
    location.startColumn,
    location.endLine - 1,
    location.endColumn + 1
  );
}

export interface QueryFragment {
  count: string;
  location: Location;
  literal?: string;
  complex?: QueryFragment[];
  type?: string;
  treeItem?: SqlQueryItem;
}

export function toCombinedString(fragment: QueryFragment): string {
  let mainPart = '';
  if (!fragment.type) {
    mainPart = fragment.literal!;
  } else {
    let delimiter = '??';
    if (fragment.type === 'AND') {
      delimiter = '';
    } else if (fragment.type === 'OR') {
      delimiter = '|';
    }
    mainPart = `(${fragment
      .complex!.map(x => toCombinedString(x))
      .join(delimiter)})`;
  }

  if (fragment.count === 'OPTIONAL') {
    mainPart += '?';
  } else if (fragment.count === 'MULTIPLE') {
    mainPart += '*';
  } else if (fragment.count === 'UNKNOWN') {
    mainPart += '??';
  }

  return mainPart;
}
