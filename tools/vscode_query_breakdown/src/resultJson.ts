export interface ResultJson {
    error_position: ErrorLocation;
    error_type: string;
    replacedFrom: string;
    replacedTo: string;
}

export interface ErrorLocation {
    startLine: number; 
    startColumn: number;
    endLine: number;
    endColumn: number;
}