// The panel of a file, which is chosen of a folder's objects: what it is,
// its size and when it changed, and its metadata, of each namespace, and
// Download, which downloads it

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import { objectURL } from '../filer/httpclient.ts';
import { objectTitle, objectType, type FilerObject, type Meta } from '../filer/schema.ts';
import { formatSize, formatTime } from './view.ts';

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

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// ObjectPanelElement is the panel of a file, which open opens
export class ObjectPanelElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        object: { state: true },
    };

    // client is the server's, of which a file is downloaded, and object is
    // the file which is open
    declare client?: Client;
    declare private object?: FilerObject;

    private readonly panel = new PanelController<string>({ closable: true });

    constructor() {
        super();
        this.panel.onClose(() => {
            this.object = undefined;
            this.dispatchEvent(new CustomEvent(ObjectPanelClosed, { bubbles: true, composed: true }));
        });
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel of the file
    open(object: FilerObject) {
        this.object = object;
        this.panel.setHeader({ title: objectTitle(object), subtitle: `${object.name}${object.path}` });
        this.panel.setActions(this.actions(object));
        this.panel.open(`${object.name}${object.path}`);
    }

    // close closes the panel, when it's open
    close() {
        if (this.object) {
            this.panel.close();
        }
    }

    override render() {
        const object = this.object;
        return html`<gs-panel .controller=${this.panel}>${object ? this.detailsView(object) : nothing}</gs-panel>`;
    }

    // actions are Download, which leaves the panel open
    private actions(object: FilerObject): PanelAction[] {
        return [{
            label: 'Download', kind: 'primary', close: false,
            run: () => {
                if (this.client) {
                    window.open(objectURL(this.client, object.name, object.path), '_blank', 'noopener');
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
