// The gf-file-view element is the objects of a folder, which aren't folders,
// as the tree has those, as go-server's gs-table, which scrolls: their
// names, each of its artwork, such as an album's cover, in place of its
// icon, when it has some, and the columns which are chosen of its toolbar's
// settings menu, which are remembered of each volume: their file names and
// formats, of a volume whose objects have them, their kinds, sizes and when
// they were changed. It loads more as it's scrolled, in the element it's
// in, which scrolls, beneath its header, which stays at the top. A row
// which is clicked is chosen, which is an event of its object, and is shown
// chosen, until it isn't selected. Of a writable volume, files are added,
// of the toolbar's Add, and its rows are selected, of their checkboxes, and
// deleted, of Carbon's batch actions, once it's confirmed.

import { LitElement, html, nothing, type PropertyValues } from 'lit';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import FileIcon from '@carbon/icons/es/document/16.js';
import type { Column, TableAction, TableSetting, TableSettingChange, TableToolbarAction } from '@mutablelogic/go-server/list/view';
import Add16 from '@carbon/icons/es/add/16.js';
import TrashCan16 from '@carbon/icons/es/trash-can/16.js';
import './delete.ts';
import './upload.ts';
import '@mutablelogic/go-server/list/view';
import { artworkURL } from '../filer/httpclient.ts';
import { objectArtwork, objectFileName, objectFormat, objectKind, objectTitle, type FilerObject } from '../filer/schema.ts';
import type { FolderController, FolderState } from './controller.ts';
import { availableColumns, columnLabel, ColumnSorts, ColumnWidths, DefaultColumns, NameSort, formatSize, formatTime, visibleColumns, type FileViewColumn } from './format.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FileViewTag is the element's tag, and FileViewSelected the event of an
// object which is chosen
export const FileViewTag = 'gf-file-view';
export const FileViewSelected = 'gf-file-view-selected';

const fileIcon = iconLoader(FileIcon);

// The prefix of the key of the columns which are chosen of a volume, in
// the browser's storage, before its name
const ColumnsKey = 'gf-file-view-columns:';

// The styles of a file's name, in gs-table's cells, which the app's styles
// don't reach: its icon, or its artwork, in a box of the same size, so the
// names line up, which artwork fills, and its title, cut short, of one line
const nameStyle = 'display: inline-flex; align-items: center; gap: 0.5rem; max-width: 100%; vertical-align: middle';
const iconStyle = 'display: inline-flex; flex: none; align-items: center; justify-content: center; width: 1.5rem; height: 1.5rem';
const artworkStyle = 'width: 100%; height: 100%; border-radius: 2px; object-fit: cover; background: var(--cds-layer-accent-01, #e0e0e0)';
const titleStyle = 'min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap';
const secondaryStyle = 'color: var(--cds-text-secondary, #525252)';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileViewElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        empty: { type: String },
        files: { type: Boolean },
        sortable: { type: Boolean },
        writable: { type: Boolean },
        columns: { state: true },
        selected: { attribute: false },
        state: { state: true },
    };

    // controller keeps the folder's objects, and empty is what's said when
    // it has none, such as to choose another folder
    declare controller?: FolderController;
    declare empty?: string;

    // files is whether the objects have file names, and formats, which are
    // shown, such as a folder's, rather than ids, such as a contact's
    declare files: boolean;

    // sortable is whether the objects can be sorted, by the server, of a
    // volume whose files are indexed, when their columns' headers are
    // clicked
    declare sortable: boolean;

    // writable is whether files can be added to the folder, and deleted, of
    // a writable volume
    declare writable: boolean;

    // columns are those which are shown, of those which can be, of the
    // volume of columnsOf
    declare private columns: FileViewColumn[];
    private columnsOf?: string;

    // selected is the path of the object which is chosen, if any
    declare selected?: string;
    declare private state?: FolderState;

    private unsubscribe?: () => void;

    constructor() {
        super();
        this.files = true;
        this.sortable = false;
        this.writable = false;
        this.columns = DefaultColumns;
    }

    // The table is in the light DOM, so the app's styles apply to it
    protected override createRenderRoot() {
        return this;
    }

    override connectedCallback() {
        super.connectedCallback();
        this.watch();
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    protected override willUpdate(changed: PropertyValues<this>) {
        if (changed.has('controller') && this.isConnected) {
            this.watch();
        }

        // The columns of the volume, which are chosen of each one
        const volume = this.state?.volume;
        if (volume && (changed.has('files') || volume !== this.columnsOf)) {
            this.columnsOf = volume;
            this.columns = visibleColumns(this.files, readColumns(volume));
        }

        // A volume which can't be sorted any more, such as once its index
        // is changed, isn't, whose order is kept until it can be, and the
        // order of a column which isn't shown is the first column's, Name,
        // of the objects' titles
        const sort = this.controller?.list.current.sort;
        if (sort && !this.sortable) {
            this.controller?.unsorted();
        } else if (sort && sort.key !== NameSort.key && !this.columns.some((column) => ColumnSorts[column]?.key === sort.key)) {
            this.controller?.list.setSort({ key: NameSort.key, direction: NameSort.first });
        }

        // An object which isn't selected any more, such as once its panel
        // closes, isn't shown chosen
        if (changed.has('selected') && !this.selected && this.state?.selected) {
            this.controller?.list.close();
        }
    }

    // watch follows the folder, and says when an object is chosen, of its
    // row, which isn't the one which is selected
    private watch() {
        this.unsubscribe?.();
        this.unsubscribe = this.controller?.subscribe((state) => {
            const chosen = state.selected;
            if (chosen && chosen !== this.state?.selected && chosen.path !== this.selected) {
                this.dispatchEvent(new CustomEvent<FilerObject>(FileViewSelected, { detail: chosen, bubbles: true, composed: true }));
            }
            this.state = state;
        });
    }

    protected override render() {
        const state = this.state;
        const controller = this.controller;
        if (!state?.volume || !controller) {
            return nothing;
        }
        // The last column which is shown, other than Name, can't be hidden
        const settings: TableSetting[] = availableColumns(this.files).map((column) => ({
            key: column, label: columnLabel(column), checked: this.columns.includes(column),
            disabled: this.columns.length === 1 && this.columns[0] === column,
        }));
        return html`
            <gs-table scrolls size="sm" class="gf-file-view-table" .controller=${controller.list} .columns=${this.tableColumns()}
                .settings=${settings} .countLabel=${countText} empty=${this.empty || 'No objects'}
                .itemKey=${(object: FilerObject) => object.path}
                .actions=${this.writable ? this.deleteActions : undefined}
                .toolbarActions=${this.writable ? this.addActions : undefined}
                @gs-table-setting=${(event: CustomEvent<TableSettingChange>) => this.show(event.detail.key as FileViewColumn, event.detail.checked)}></gs-table>
            <gf-object-delete .client=${controller.client}></gf-object-delete>
            <gf-object-upload .client=${controller.client}></gf-object-upload>
        `;
    }

    // addActions are Add, in the toolbar, which opens the panel which adds
    // files to the folder, which is loaded again, once they're added
    private readonly addActions: TableToolbarAction[] = [{
        label: 'Add', kind: 'primary', icon: Add16,
        run: () => {
            const { volume, path } = this.state ?? {};
            if (volume && path) {
                this.querySelector('gf-object-upload')?.open(volume, path, () => void this.controller?.reload());
            }
        },
    }];

    // deleteActions are Delete, of the rows which are selected, of Carbon's
    // batch actions, whose buttons are primary, on its bar, which asks to
    // confirm, of a panel whose Delete is danger, and deletes them, and loads the folder again, or keeps them
    // selected, when it isn't confirmed
    private readonly deleteActions: TableAction<FilerObject>[] = [{
        label: 'Delete', kind: 'primary', icon: TrashCan16,
        run: async (objects) => {
            const panel = this.querySelector('gf-object-delete');
            const deleted = panel ? await panel.confirm(objects) : false;
            if (deleted) {
                void this.controller?.reload();
            }
            if (!deleted) {
                throw new DeleteCancelled();
            }
        },
    }];

    // tableColumns are the table's columns: Name, which has what's left of
    // its width, and those which are shown, of their widths, which sort the
    // objects, by the server, when they're sortable
    private tableColumns(): Column<FilerObject>[] {
        const name: Column<FilerObject> = {
            header: 'Name',
            cell: (object) => this.nameView(object),
            sort: this.sortable ? NameSort.key : undefined,
            sortFirst: this.sortable ? NameSort.first : undefined,
        };
        return [name, ...this.columns.map((column) => {
            const sort = this.sortable ? ColumnSorts[column] : undefined;
            return {
                header: columnLabel(column),
                width: `${ColumnWidths[column]}rem`,
                align: column === 'size' ? 'end' as const : undefined,
                sort: sort?.key,
                sortFirst: sort?.first,
                cell: (object: FilerObject) => cellView(column, object),
            };
        })];
    }

    // nameView is a file's name: its artwork which shows it best, such as an
    // album's cover, which is loaded once it's scrolled to, or else its icon,
    // and its title, whose path is shown when it's pointed at
    private nameView(object: FilerObject) {
        const artwork = objectArtwork(object);
        const client = this.controller?.client;
        const icon = artwork && client
            ? html`<img src=${artworkURL(client, artwork.hash)} alt="" loading="lazy" decoding="async" style=${artworkStyle}>`
            : fileIcon;
        return html`<span style=${nameStyle} title=${object.path}><span style=${iconStyle}>${icon}</span><span style=${titleStyle}>${objectTitle(object)}</span></span>`;
    }

    // show shows, or hides, a column, which is remembered, of the volume
    private show(column: FileViewColumn, shown: boolean) {
        if (this.columns.includes(column) === shown || (!shown && this.columns.length === 1)) {
            return;
        }
        const chosen = shown ? [...this.columns, column] : this.columns.filter((c) => c !== column);
        this.columns = visibleColumns(this.files, chosen);
        if (this.columnsOf) {
            writeColumns(this.columnsOf, this.columns);
        }
    }
}

if (!customElements.get(FileViewTag)) {
    customElements.define(FileViewTag, FileViewElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [FileViewTag]: FileViewElement;
    }
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// The file view's columns, and its sizes, and times, for people
export { availableColumns, formatSize, formatTime, visibleColumns, type FileViewColumn };

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// cellView is a file's cell of a column
function cellView(column: FileViewColumn, object: FilerObject) {
    switch (column) {
        case 'file':
            return html`<span style=${secondaryStyle}>${objectFileName(object)}</span>`;
        case 'kind':
            return objectKind(object);
        case 'format':
            return html`<span title=${object.type ?? ''}>${objectFormat(object)}</span>`;
        case 'size':
            return formatSize(object.size);
        case 'modified':
            return formatTime(object.mtime);
    }
}

// DeleteCancelled is why files which are selected aren't deleted, as it
// wasn't confirmed, which keeps them selected, and isn't an error
class DeleteCancelled extends Error {}

// countText returns how many objects a folder has
function countText(count: number): string {
    return `${count.toLocaleString()} ${count === 1 ? 'item' : 'items'}`;
}

// readColumns returns the columns which were chosen of a volume, or
// undefined, when none were, or the browser's storage can't be read
function readColumns(volume: string): string[] | undefined {
    try {
        const value = JSON.parse(localStorage.getItem(ColumnsKey + volume) ?? 'null');
        return Array.isArray(value) ? value.filter((v) => typeof v === 'string') : undefined;
    } catch {
        return undefined;
    }
}

// writeColumns remembers the columns which are chosen of a volume, when the
// browser's storage can be written
function writeColumns(volume: string, columns: readonly FileViewColumn[]) {
    try {
        localStorage.setItem(ColumnsKey + volume, JSON.stringify(columns));
    } catch {
        // It isn't remembered
    }
}
