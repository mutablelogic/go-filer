// The panel of a file, which is chosen of a folder's objects, of its tabs:
// Metadata, what it is, its size and when it changed, and its metadata, of
// each namespace, and, of a file which has them, Artwork, its images, such
// as an album's cover, Map, where it is, and Events, its times, and
// Preview, of one whose content can be read as Markdown.
// Download downloads it, of each tab, and Play plays audio, or video, in
// the sidebar's player.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction, type PanelTab } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import { play, playable } from '@mutablelogic/go-server/player';
import Download16 from '@carbon/icons/es/download/16.js';
import Play16 from '@carbon/icons/es/play/16.js';
import { artworkURL, objectURL } from '../filer/httpclient.ts';
import { artworkLabel, metaValue, MetadataNamespace, objectTitle, objectType, type Artwork, type FilerObject, type Meta } from '../filer/schema.ts';
import { formatSize, formatTime } from './view.ts';
import './preview.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ObjectTab is a tab of a file's panel
export type ObjectTab = 'metadata' | 'artwork' | 'map' | 'events' | 'preview';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const ObjectPanelTag = 'gf-object-panel';

// ObjectPanelClosed is the event of the panel, once it's closed
export const ObjectPanelClosed = 'gf-object-panel-closed';

// The namespaces of metadata which are shown first, in this order, and the
// others after them, of their names
const FirstNamespaces = ['dc', 'metadata'];

// A value which is a time, such as 2023-12-26T04:34:45Z, or a link
const reTime = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/;
const reLink = /^https?:\/\//;

// The key of a file's hints, of what's read of it next, and the hint of
// its content, as Markdown, of go-metadata
const HintKey = 'hint';
const HintMarkdown = 'markdown';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// ObjectPanelElement is the panel of a file, which open opens
export class ObjectPanelElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        object: { state: true },
        tab: { state: true },
    };

    // client is the server's, of which a file is downloaded, and object is
    // the file which is open
    declare client?: Client;
    declare private object?: FilerObject;

    // tab is the tab which is chosen
    declare private tab?: ObjectTab;

    private readonly panel = new PanelController<string>({ closable: true });

    constructor() {
        super();
        this.panel.subscribe((state) => { this.tab = state.tab as ObjectTab | undefined; });
        this.panel.onClose(() => {
            this.object = undefined;
            this.dispatchEvent(new CustomEvent(ObjectPanelClosed, { bubbles: true, composed: true }));
        });
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel of the file, of the tab which is chosen, when the
    // file has it, or else of its metadata
    open(object: FilerObject) {
        this.object = object;
        const tabs = this.tabs(object);
        const tab = tabs.find((t) => t.value === this.tab && !t.disabled)?.value ?? 'metadata';
        this.panel.setHeader({ title: objectTitle(object), subtitle: `${object.name}${object.path}` });
        this.panel.setTabs(tabs, tab);
        this.panel.open(`${object.name}${object.path}`);
    }

    // refresh shows the file again, as it is now, such as once its folder
    // is loaded again, of the tab which is chosen, when it still has it,
    // without opening the panel, when it's closed
    refresh(object: FilerObject) {
        if (!this.object) {
            return;
        }
        this.object = object;
        const tabs = this.tabs(object);
        const tab = tabs.find((t) => t.value === this.tab && !t.disabled)?.value ?? 'metadata';
        this.panel.setHeader({ title: objectTitle(object), subtitle: `${object.name}${object.path}` });
        this.panel.setTabs(tabs, tab);
    }

    // close closes the panel, when it's open
    close() {
        if (this.object) {
            this.panel.close();
        }
    }

    override render() {
        const object = this.object;
        return html`<gs-panel .controller=${this.panel}>${object ? this.tabView(object) : nothing}</gs-panel>`;
    }

    // tabs are Metadata, and Artwork, Map and Events, only of a file which
    // has them, and Preview, of one whose content can be read as Markdown,
    // each of Download
    private tabs(object: FilerObject): PanelTab[] {
        const actions = this.actions(object);
        const tabs: PanelTab[] = [{ value: 'metadata', label: 'Metadata', actions }];
        if (object.artwork?.length) {
            tabs.push({ value: 'artwork', label: 'Artwork', actions });
        }
        if (object.geo?.length) {
            tabs.push({ value: 'map', label: 'Map', actions });
        }
        if (object.events?.length) {
            tabs.push({ value: 'events', label: 'Events', actions });
        }
        tabs.push({ value: 'preview', label: 'Preview', disabled: !hinted(object, HintMarkdown), actions });
        return tabs;
    }

    // tabView is the tab which is chosen: the file's metadata, or its
    // artwork, or where it is, or its events, which are to come, or its
    // preview, of its content, as Markdown
    private tabView(object: FilerObject): TemplateResult | typeof nothing {
        switch (this.tab) {
            case 'artwork':
                return this.artworkView(object.artwork ?? []);
            case 'map':
                return html`<p class="gf-empty">${countOf(object.geo?.length ?? 0, 'place')}</p>`;
            case 'events':
                return html`<p class="gf-empty">${countOf(object.events?.length ?? 0, 'event')}</p>`;
            case 'preview':
                return html`<gf-object-preview .client=${this.client} .object=${object}></gf-object-preview>`;
            default:
                return this.detailsView(object);
        }
    }

    // actions are Play, of audio, or video, and Download, which leave the
    // panel open
    private actions(object: FilerObject): PanelAction[] {
        const client = this.client;
        if (!client) {
            return [];
        }
        const url = objectURL(client, object.name, object.path);
        const download: PanelAction = {
            label: 'Download', kind: playable(object.type) ? 'secondary' : 'primary', icon: Download16, close: false,
            run: () => { window.open(objectURL(client, object.name, object.path, true), '_blank', 'noopener'); },
        };
        if (!playable(object.type)) {
            return [download];
        }
        return [download, {
            label: 'Play', kind: 'primary', icon: Play16, close: false,
            run: () => play({ url, name: objectTitle(object), type: object.type }, this),
        }];
    }

    // artworkView is the file's artwork, in the order of their contexts,
    // each of what it is, such as Cover, its size, in pixels, and its type,
    // which opens on its own, of its own size, when it's clicked
    private artworkView(artwork: readonly Artwork[]): TemplateResult | typeof nothing {
        const client = this.client;
        if (!client) {
            return nothing;
        }
        return html`
            <ul class="gf-object-artwork">
                ${artwork.map((a) => {
                    const url = artworkURL(client, a.hash);
                    const label = artworkLabel(a);
                    return html`
                        <li>
                            <a href=${url} target="_blank" rel="noopener" title=${`Open ${label}`}>
                                <img src=${url} alt=${label} width=${a.width} height=${a.height} loading="lazy" decoding="async">
                            </a>
                            <span class="gf-object-artwork-label">${label}</span>
                            <span class="gf-object-artwork-detail">${[`${a.width} × ${a.height}`, a.type.replace(/^image\//, '').toUpperCase(), formatSize(a.size)].filter(Boolean).join(' · ')}</span>
                        </li>
                    `;
                })}
            </ul>
        `;
    }

    // detailsView is what the file is, and its metadata, of each namespace
    private detailsView(object: FilerObject): TemplateResult {
        const rows: [string, string][] = [
            ['Type', [objectType(object), object.type].filter(Boolean).join(' · ') || '–'],
            ['Size', formatSize(object.size) || '–'],
            ['Modified', formatTime(object.mtime) || '–'],
        ];
        if (object.etag) {
            rows.push(['ETag', object.etag]);
        }
        const namespaces = metaNamespaces(object.meta ?? []);
        return html`
            <dl class="gf-volume-details">
                ${rows.map(([key, value]) => html`<dt>${key}</dt><dd>${value}</dd>`)}
            </dl>
            ${namespaces.map(([namespace, meta]) => html`
                <h4 class="gf-object-namespace">${namespace}</h4>
                <dl class="gf-volume-details">
                    ${meta.map((m) => html`<dt>${m.key}</dt><dd>${metaView(m.text || m.value)}</dd>`)}
                </dl>
            `)}
        `;
    }
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// countOf returns how many there are of a noun, such as 2 events
function countOf(count: number, noun: string): string {
    return `${count.toLocaleString()} ${count === 1 ? noun : noun + 's'}`;
}

// hinted returns whether a file's metadata hints the hint, such as that it
// has artwork
function hinted(object: FilerObject, hint: string): boolean {
    const hints = metaValue(object, MetadataNamespace, HintKey);
    return Array.isArray(hints) && hints.includes(hint);
}

// metaNamespaces returns the metadata of each namespace, those of
// FirstNamespaces first, and the others of their names
function metaNamespaces(meta: readonly Meta[]): [string, Meta[]][] {
    const namespaces = new Map<string, Meta[]>();
    for (const m of meta) {
        const namespace = m.namespace || '–';
        namespaces.set(namespace, [...(namespaces.get(namespace) ?? []), m]);
    }
    const rank = (namespace: string) => {
        const i = FirstNamespaces.indexOf(namespace);
        return i < 0 ? FirstNamespaces.length : i;
    };
    return [...namespaces].sort(([a], [b]) => rank(a) - rank(b) || a.localeCompare(b));
}

// metaView returns a value of metadata, or its text, in preference: a list, of its values, a time, in
// the browser's locale, a link, which opens elsewhere, or its JSON
function metaView(value: unknown): TemplateResult | string {
    if (Array.isArray(value)) {
        return value.every((v) => typeof v !== 'object' || v === null)
            ? value.map(String).join(', ')
            : JSON.stringify(value);
    } else if (typeof value === 'string' && reLink.test(value)) {
        return html`<a href=${value} target="_blank" rel="noopener noreferrer">${value}</a>`;
    } else if (typeof value === 'string' && reTime.test(value)) {
        return formatTime(value) || value;
    } else if (typeof value === 'object' && value !== null) {
        return JSON.stringify(value);
    }
    return String(value ?? '');
}

if (!customElements.get(ObjectPanelTag)) {
    customElements.define(ObjectPanelTag, ObjectPanelElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectPanelTag]: ObjectPanelElement;
    }
}
