// The gf-file-view element is the objects of a folder, which aren't folders,
// as the tree has those, as a table of their names, types, sizes and when
// they were changed, which loads more as it's scrolled, in the element it's in,
// which scrolls.

import { LitElement, html, nothing, type PropertyValues } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import FileIcon from '@carbon/icons/es/document/16.js';
import EmptyIcon from '@carbon/icons/es/folders/32.js';
import { objectTitle, objectType } from '../filer/schema.ts';
import type { FolderController, FolderState } from './controller.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FileViewTag is the element's tag
export const FileViewTag = 'gf-file-view';

const fileIcon = iconLoader(FileIcon);

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileViewElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        empty: { type: String },
        state: { state: true },
    };

    // controller keeps the folder's objects, and empty is what's said when
    // it has none, such as to choose another folder
    declare controller?: FolderController;
    declare empty?: string;
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
            return html`
                <div class="gf-file-view-empty">
                    ${iconLoader(EmptyIcon)}
                    <p>${this.empty || 'No objects'}</p>
                </div>
            `;
        }
        // A table of Carbon's styles, rather than its element, whose own
        // scrolling would keep its header from staying at the top of the
        // element which scrolls
        const count = state.count;
        return html`
            <table class="cds--data-table cds--data-table--sm gf-file-view-table">
                <thead>
                    <tr>
                        <th><div class="cds--table-header-label">Name</div></th>
                        <th class="gf-file-view-type"><div class="cds--table-header-label">Type</div></th>
                        <th class="gf-file-view-size"><div class="cds--table-header-label">Size</div></th>
                        <th><div class="cds--table-header-label">Modified</div></th>
                    </tr>
                </thead>
                <tbody>
                    ${repeat(state.objects, (object) => object.path, (object) => html`
                        <tr>
                            <td>
                                <span class="gf-file-view-name" title=${object.path}>${fileIcon}<span>${objectTitle(object)}</span></span>
                            </td>
                            <td class="gf-file-view-type" title=${object.type ?? ''}>${objectType(object)}</td>
                            <td class="gf-file-view-size">${formatSize(object.size)}</td>
                            <td>${formatTime(object.mtime)}</td>
                        </tr>
                    `)}
                </tbody>
            </table>
            ${state.more ? html`<p class="gf-file-view-end gf-empty">${state.loading ? 'Loading…' : ''}</p>` : nothing}
            ${count !== null ? html`<p class="gf-file-view-count">${countText(count)}</p>` : nothing}
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
