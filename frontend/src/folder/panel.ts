// The panel of a file, which is chosen of a folder's objects, of three
// tabs: Metadata, what it is, its size and when it changed, and its
// metadata, of each namespace, Artwork, of a file whose metadata hints it
// has artwork, and Preview, of one whose content can be read as Markdown.
// Download downloads it, of each tab.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction, type PanelTab } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import { objectURL } from '../filer/httpclient.ts';
import { metaValue, MetadataNamespace, objectTitle, objectType, type FilerObject, type Meta } from '../filer/schema.ts';
import { formatSize, formatTime } from './view.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ObjectTab is a tab of a file's panel
export type ObjectTab = 'metadata' | 'artwork' | 'preview';

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

// The key of a file's hints, of what's read of it next, and the hints of
// its artwork, and its content, as Markdown, of go-metadata
const HintKey = 'hint';
const HintArtwork = 'artwork';
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

    // tabs are Metadata, Artwork, of a file whose metadata hints it has
    // artwork, and Preview, of one whose content can be read as Markdown,
    // each of Download
    private tabs(object: FilerObject): PanelTab[] {
        const actions = this.actions(object);
        return [
            { value: 'metadata', label: 'Metadata', actions },
            { value: 'artwork', label: 'Artwork', disabled: !hinted(object, HintArtwork), actions },
            { value: 'preview', label: 'Preview', disabled: !hinted(object, HintMarkdown), actions },
        ];
    }

    // tabView is the tab which is chosen: the file's metadata, or its
    // artwork, or its preview, which are empty, for now
    private tabView(object: FilerObject): TemplateResult | typeof nothing {
        switch (this.tab) {
            case 'artwork':
            case 'preview':
                return nothing;
            default:
                return this.detailsView(object);
        }
    }

    // actions are Download, which leaves the panel open
    private actions(object: FilerObject): PanelAction[] {
        return [{
            label: 'Download', kind: 'primary', close: false,
            run: () => {
                if (this.client) {
                    window.open(objectURL(this.client, object.name, object.path, true), '_blank', 'noopener');
                }
            },
        }];
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
                    ${meta.map((m) => html`<dt>${m.key}</dt><dd>${metaView(m.value)}</dd>`)}
                </dl>
            `)}
        `;
    }
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

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

// metaView returns a value of metadata: a list, of its values, a time, in
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
