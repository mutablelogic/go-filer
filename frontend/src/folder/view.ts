// The gf-file-view element is the objects of a folder, which aren't folders,
// as the tree has those, as Carbon's data table of their names, and of the
// columns which are chosen of its toolbar's Columns menu: their file names
// and formats, of a volume whose objects have them, their kinds, sizes and
// when they were changed, each of its artwork, such as an album's cover, in place
// of its icon, when it has some, which loads more as it's scrolled, in the element it's in,
// which scrolls. A row which is clicked is chosen, which is an event of
// its object, and is shown chosen, of its path, as selected. Its name is a
// button, so it's chosen of the keyboard, too.

import { LitElement, html, nothing, type PropertyValues } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import FileIcon from '@carbon/icons/es/document/16.js';
import EmptyIcon from '@carbon/icons/es/folders/32.js';
import { artworkURL } from '../filer/httpclient.ts';
import { objectArtwork, objectFileName, objectFormat, objectKind, objectTitle, type FilerObject } from '../filer/schema.ts';
import type { FolderController, FolderState } from './controller.ts';
import { availableColumns, columnLabel, ColumnWidths, DefaultColumns, formatSize, formatTime, tableWidth, visibleColumns, type FileViewColumn } from './format.ts';
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/data-table/index.js';
import '@carbon/web-components/es/components/menu-button/index.js';
import '@carbon/web-components/es/components/menu/index.js';

///////////////////////////////////////////////////////////////////////////////
// TYPES

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

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileViewElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        empty: { type: String },
        files: { type: Boolean },
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

    // columns are those which are shown, of those which can be, of the
    // volume of columnsOf
    declare private columns: FileViewColumn[];
    private columnsOf?: string;

    // selected is the path of the object which is chosen, if any
    declare selected?: string;
    declare private state?: FolderState;

    constructor() {
        super();
        this.files = true;
        this.columns = DefaultColumns;
    }

    private unsubscribe?: () => void;
    private observer?: IntersectionObserver;

    // The table is in the light DOM, so the app's styles apply
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
        this.observer?.disconnect();
        this.observer = undefined;
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
    }

    private watch() {
        this.unsubscribe?.();
        this.unsubscribe = this.controller?.subscribe((state) => { this.state = state; });
    }

    // More are loaded when the end of the table is scrolled to, which is
    // watched once it's rendered
    protected override updated() {
        const end = this.querySelector('.gf-file-view-end');
        this.observer?.disconnect();
        if (end && this.state?.more && !this.state.failed) {
            this.observer = new IntersectionObserver((entries) => {
                if (entries.some((entry) => entry.isIntersecting)) {
                    void this.controller?.loadMore();
                }
            }, { rootMargin: '200px' });
            this.observer.observe(end);
        }
    }

    protected override render() {
        const state = this.state;
        if (!state?.volume) {
            return nothing;
        } else if (!state.objects) {
            return html`<p class="gf-empty">Loading…</p>`;
        } else if (state.objects.length === 0 && state.failed) {
            return this.failedView();
        } else if (state.objects.length === 0) {
            return html`
                <div class="gf-file-view-empty">
                    ${iconLoader(EmptyIcon)}
                    <p>${this.empty || 'No objects'}</p>
                </div>
            `;
        }
        // Carbon's table, whose header stays at the top of the element which
        // scrolls, as its own scrolling is turned off (see index.html)
        const count = state.count;
        const columns = this.columns;
        return html`
            <cds-table size="sm" class="gf-file-view-table" style=${`--gf-file-view-width: ${tableWidth(columns)}rem`}>
                <cds-table-toolbar slot="toolbar" size="sm">
                    <cds-table-toolbar-content>
                        <cds-menu-button kind="ghost" size="sm" label="Columns" menu-alignment="bottom-end">
                            <cds-menu>
                                ${availableColumns(this.files).map((column) => html`
                                    <cds-menu-item-selectable label=${columnLabel(column)} ?selected=${columns.includes(column)}
                                        @cds-item-changed=${(event: Event) => this.show(column, (event.target as HTMLElement & { selected: boolean }).selected)}></cds-menu-item-selectable>
                                `)}
                            </cds-menu>
                        </cds-menu-button>
                    </cds-table-toolbar-content>
                </cds-table-toolbar>
                <cds-table-head>
                    <cds-table-header-row>
                        <cds-table-header-cell class="gf-file-view-title">Name</cds-table-header-cell>
                        ${columns.map((column) => html`<cds-table-header-cell class=${ColumnClasses[column]} style=${`width: ${ColumnWidths[column]}rem`}>${columnLabel(column)}</cds-table-header-cell>`)}
                    </cds-table-header-row>
                </cds-table-head>
                <cds-table-body>
                    ${repeat(state.objects, (object) => object.path, (object) => html`
                        <cds-table-row size="sm" ?selected=${object.path === this.selected} @click=${() => this.choose(object)}>
                            <cds-table-cell class="gf-file-view-title">
                                <button type="button" class="gf-file-view-name" title=${object.path}><span class="gf-file-view-icon">${this.iconView(object)}</span><span>${objectTitle(object)}</span></button>
                            </cds-table-cell>
                            ${columns.map((column) => this.cellView(column, object))}
                        </cds-table-row>
                    `)}
                </cds-table-body>
            </cds-table>
            ${state.failed ? this.failedView() : state.more ? html`<p class="gf-file-view-end gf-empty">${state.loading ? 'Loading…' : ''}</p>` : nothing}
            ${count !== null ? html`<p class="gf-file-view-count">${countText(count)}</p>` : nothing}
        `;
    }

    // cellView is a file's cell of a column
    private cellView(column: FileViewColumn, object: FilerObject) {
        switch (column) {
            case 'file':
                return html`<cds-table-cell class=${ColumnClasses.file}>${objectFileName(object)}</cds-table-cell>`;
            case 'kind':
                return html`<cds-table-cell class=${ColumnClasses.kind}>${objectKind(object)}</cds-table-cell>`;
            case 'format':
                return html`<cds-table-cell class=${ColumnClasses.format} title=${object.type ?? ''}>${objectFormat(object)}</cds-table-cell>`;
            case 'size':
                return html`<cds-table-cell class=${ColumnClasses.size}>${formatSize(object.size)}</cds-table-cell>`;
            case 'modified':
                return html`<cds-table-cell class=${ColumnClasses.modified}>${formatTime(object.mtime)}</cds-table-cell>`;
        }
    }

    // show shows, or hides, a column, which is remembered, of the volume.
    // Carbon's item says it changed twice, so it's of whether it's
    // selected, rather than toggled.
    private show(column: FileViewColumn, shown: boolean) {
        if (this.columns.includes(column) === shown) {
            return;
        }
        const chosen = shown ? [...this.columns, column] : this.columns.filter((c) => c !== column);
        this.columns = visibleColumns(this.files, chosen);
        if (this.columnsOf) {
            writeColumns(this.columnsOf, this.columns);
        }
    }

    // iconView is a file's artwork which shows it best, such as an album's
    // cover, which is loaded once it's scrolled to, or else its icon
    private iconView(object: FilerObject) {
        const artwork = objectArtwork(object);
        const client = this.controller?.client;
        if (!artwork || !client) {
            return fileIcon;
        }
        return html`<img src=${artworkURL(client, artwork.hash)} alt="" loading="lazy" decoding="async">`;
    }

    // choose says the object is chosen
    private choose(object: FilerObject) {
        this.dispatchEvent(new CustomEvent<FilerObject>(FileViewSelected, { detail: object, bubbles: true, composed: true }));
    }

    // failedView says the objects couldn't be loaded, and loads them again,
    // from where they couldn't be, when it's asked to
    private failedView() {
        return html`
            <div class="gf-file-view-failed">
                <span>${this.state?.objects?.length ? "More objects couldn't be loaded" : "The objects couldn't be loaded"}</span>
                <cds-button kind="ghost" size="sm" @click=${() => void this.controller?.loadMore()}>Try again</cds-button>
            </div>
        `;
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



// countText returns how many objects a folder has
function countText(count: number): string {
    return `${count.toLocaleString()} ${count === 1 ? 'item' : 'items'}`;
}

// ColumnClasses are the classes of the columns' cells
const ColumnClasses: Record<FileViewColumn, string> = { file: 'gf-file-view-file', kind: 'gf-file-view-kind', format: 'gf-file-view-format', size: 'gf-file-view-size', modified: 'gf-file-view-modified' };


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
