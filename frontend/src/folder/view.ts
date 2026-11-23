// The gf-file-view element is the objects of a folder, which aren't folders,
// as the tree has those, as Carbon's data table of their names, types, sizes and when
// they were changed, each of its artwork, such as an album's cover, in place
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
import { objectArtwork, objectTitle, objectType, type FilerObject } from '../filer/schema.ts';
import type { FolderController, FolderState } from './controller.ts';
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/data-table/index.js';

///////////////////////////////////////////////////////////////////////////////
// TYPES

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FileViewTag is the element's tag, and FileViewSelected the event of an
// object which is chosen
export const FileViewTag = 'gf-file-view';
export const FileViewSelected = 'gf-file-view-selected';

const fileIcon = iconLoader(FileIcon);

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileViewElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        empty: { type: String },
        selected: { attribute: false },
        state: { state: true },
    };

    // controller keeps the folder's objects, and empty is what's said when
    // it has none, such as to choose another folder
    declare controller?: FolderController;
    declare empty?: string;

    // selected is the path of the object which is chosen, if any
    declare selected?: string;
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
        return html`
            <cds-table size="sm" class="gf-file-view-table">
                <cds-table-head>
                    <cds-table-header-row>
                        <cds-table-header-cell>Name</cds-table-header-cell>
                        <cds-table-header-cell class="gf-file-view-type">Type</cds-table-header-cell>
                        <cds-table-header-cell class="gf-file-view-size">Size</cds-table-header-cell>
                        <cds-table-header-cell>Modified</cds-table-header-cell>
                    </cds-table-header-row>
                </cds-table-head>
                <cds-table-body>
                    ${repeat(state.objects, (object) => object.path, (object) => html`
                        <cds-table-row size="sm" ?selected=${object.path === this.selected} @click=${() => this.choose(object)}>
                            <cds-table-cell>
                                <button type="button" class="gf-file-view-name" title=${object.path}><span class="gf-file-view-icon">${this.iconView(object)}</span><span>${objectTitle(object)}</span></button>
                            </cds-table-cell>
                            <cds-table-cell class="gf-file-view-type" title=${object.type ?? ''}>${objectType(object)}</cds-table-cell>
                            <cds-table-cell class="gf-file-view-size">${formatSize(object.size)}</cds-table-cell>
                            <cds-table-cell>${formatTime(object.mtime)}</cds-table-cell>
                        </cds-table-row>
                    `)}
                </cds-table-body>
            </cds-table>
            ${state.failed ? this.failedView() : state.more ? html`<p class="gf-file-view-end gf-empty">${state.loading ? 'Loading…' : ''}</p>` : nothing}
            ${count !== null ? html`<p class="gf-file-view-count">${countText(count)}</p>` : nothing}
        `;
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
