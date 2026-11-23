// What the file view shows of objects: its columns, and sizes, and times,
// for people, which don't need a browser, so they're tested without one

///////////////////////////////////////////////////////////////////////////////
// TYPES

// FileViewColumn is a column of the file view, apart from Name, which is
// always shown
export type FileViewColumn = 'file' | 'kind' | 'format' | 'size' | 'modified';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The columns, in order, of their labels, those of file names and formats,
// which are only of volumes whose objects have them, and those which are
// shown, until others are chosen
const Columns: [FileViewColumn, string][] = [['file', 'File'], ['kind', 'Kind'], ['format', 'Format'], ['size', 'Size'], ['modified', 'Modified']];
const FileColumns: FileViewColumn[] = ['file', 'format'];
export const DefaultColumns: FileViewColumn[] = ['kind', 'size', 'modified'];

// ColumnWidths are the widths of the columns, in rem, and NameWidth the
// least width of Name, which has what's left of the table's
export const ColumnWidths: Record<FileViewColumn, number> = { file: 12, kind: 7, format: 5, size: 5.5, modified: 10.5 };
export const NameWidth = 12;

///////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// availableColumns returns the columns which can be shown, in order, of a
// volume whose objects have file names, or not
export function availableColumns(files: boolean): FileViewColumn[] {
    return Columns.map(([column]) => column).filter((column) => files || !FileColumns.includes(column));
}

// tableWidth returns the least width of the table, in rem, of the columns
// which are shown, and Name
export function tableWidth(columns: readonly FileViewColumn[]): number {
    return columns.reduce((width, column) => width + ColumnWidths[column], NameWidth);
}

// visibleColumns returns the columns which are shown, in order, of those
// which are chosen, which can be, or else the default ones
export function visibleColumns(files: boolean, chosen?: readonly string[]): FileViewColumn[] {
    const available = availableColumns(files);
    return available.filter((column) => (chosen ?? DefaultColumns).includes(column));
}

// columnLabel returns a column's label
export function columnLabel(column: FileViewColumn): string {
    return Columns.find(([c]) => c === column)?.[1] ?? column;
}

// formatSize returns a size, in bytes, as B, KB, MB, GB or TB, of 1,000s,
// or "" when it isn't known
export function formatSize(size?: number): string {
    if (size === undefined || size < 0) {
        return '';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = size;
    let unit = 0;
    while (value >= 1000 && unit < units.length - 1) {
        value /= 1000;
        unit++;
    }
    return unit === 0 ? `${value} B` : `${value.toFixed(value < 10 ? 1 : 0)} ${units[unit]}`;
}

// formatTime returns when something changed, in the browser's locale, or ""
// when it isn't known
export function formatTime(time?: string): string {
    if (!time) {
        return '';
    }
    const date = new Date(time);
    return Number.isNaN(date.getTime()) || date.getFullYear() <= 1 ? '' : date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}
