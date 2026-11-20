// The gf-file-view element is the objects of a folder, its folders and its
// files, as a table of their names, sizes and when they were changed, which
// loads more as it's scrolled, in the element it's in, which scrolls. A
// folder which is chosen fires gf-file-view-opened, with its volume and path.

import { LitElement, html, nothing, type PropertyValues } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import FolderIcon from '@carbon/icons/es/folder/16.js';
import FileIcon from '@carbon/icons/es/document/16.js';
import { objectTitle, type FilerObject } from '../filer/schema.ts';
import type { FolderController, FolderState } from './controller.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// FileViewOpened is a folder which is chosen: its volume, and its path
export interface FileViewOpened {
    volume: string;
    path: string;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FileViewTag is the element's tag, and FileViewOpenedEvent the event it
// fires, with a FileViewOpened, when a folder is chosen
export const FileViewTag = 'gf-file-view';
export const FileViewOpenedEvent = 'gf-file-view-opened';

const Icons = {
    folder: iconLoader(FolderIcon),
    file: iconLoader(FileIcon),
};

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileViewElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        state: { state: true },
    };

    // controller keeps the folder's objects
    declare controller?: FolderController;
    declare private state?: FolderState;

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
        if (end && this.state?.more) {
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
        } else if (state.objects.length === 0) {
            return html`<p class="gf-empty">This folder is empty</p>`;
        }
        const volume = state.volume;
        // A table of Carbon's styles, rather than its element, whose own
        // scrolling would keep its header from staying at the top of the
        // element which scrolls
        const count = state.count ?? (state.more ? null : state.objects.length);
        return html`
            <table class="cds--data-table cds--data-table--sm gf-file-view-table">
                <thead>
                    <tr>
                        <th><div class="cds--table-header-label">Name</div></th>
                        <th class="gf-file-view-size"><div class="cds--table-header-label">Size</div></th>
                        <th><div class="cds--table-header-label">Modified</div></th>
                    </tr>
                </thead>
                <tbody>
                    ${repeat(state.objects, (object) => (object.dir ? 'd:' : 'f:') + object.path, (object) => html`
                        <tr class=${object.dir ? 'gf-file-view-folder' : ''} @click=${() => object.dir && this.opened(volume, object)}>
                            <td>
                                <span class="gf-file-view-name" title=${object.path}>${object.dir ? Icons.folder : Icons.file}<span>${objectTitle(object)}</span></span>
                            </td>
                            <td class="gf-file-view-size">${object.dir ? '' : formatSize(object.size)}</td>
                            <td>${formatTime(object.mtime)}</td>
                        </tr>
                    `)}
                </tbody>
            </table>
            ${state.more ? html`<p class="gf-file-view-end gf-empty">${state.loading ? 'Loading…' : ''}</p>` : nothing}
            ${count !== null ? html`<p class="gf-file-view-count">${countText(count)}</p>` : nothing}
        `;
    }

    // opened fires that a folder is chosen
    private opened(volume: string, folder: FilerObject) {
        this.dispatchEvent(new CustomEvent<FileViewOpened>(FileViewOpenedEvent, { detail: { volume, path: folder.path }, bubbles: true, composed: true }));
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

// countText returns how many objects a folder has
function countText(count: number): string {
    return `${count.toLocaleString()} ${count === 1 ? 'item' : 'items'}`;
}
