/**
 * resultJson.ts contains the JSON representation of unparseable components
 */
export interface ResultJson {
  error_position: ErrorLocation;
  error_type: string;
  replacedFrom: string;
  replacedTo: string;
  performance: string;
  runtime: string;
}

export interface ErrorLocation {
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}
