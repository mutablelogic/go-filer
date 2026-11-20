// The Files module: a page of the volumes, which is where the file tree,
// and the file view, of a volume's objects, will be

import { LitElement, html, nothing } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import type { NavController } from '@mutablelogic/go-server/nav';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import type { Router } from '@mutablelogic/go-server/router';
import Folder32 from '@carbon/icons/es/folder/32.js';
import { listVolumes } from '../filer/httpclient.ts';
import { RegistryChangeSchema, StoreChangeSchema, VolumeChangeTable, type Volume } from '../filer/schema.ts';
import '@mutablelogic/go-server/page/view';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FilesTag is the tag of the Files page
export const FilesTag = 'gf-files';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FilesElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        notifications: { attribute: false },
        changes: { attribute: false },
        volumes: { state: true },
    };

    // client is the filer API's client, notifications show its errors, and
    // changes are followed, so the volumes are loaded again when they, or
    // what the registry knows of them, such as whether they're mounted,
    // change
    declare client?: Client;
    declare notifications?: NotificationController;
    declare changes?: ChangesController;

    // volumes are the volumes, once they're loaded
    declare private volumes?: Volume[];

    private unsubscribe?: () => void;
    private abort?: AbortController;

    // The page is in the light DOM, so the app's styles apply
    protected override createRenderRoot() {
        return this;
    }

    // The volumes are loaded at once, rather than once the changes are
    // watched, so they're shown when they can't be
    override connectedCallback() {
        super.connectedCallback();
        this.load();
        this.unsubscribe = this.changes?.subscribe([
            { schema: StoreChangeSchema, table: VolumeChangeTable },
            { schema: RegistryChangeSchema, table: VolumeChangeTable },
        ], () => this.load());
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.unsubscribe?.();
        this.abort?.abort();
    }

    // load loads the volumes, aborting a load which is pending
    async load() {
        if (!this.client) {
            return;
        }
        this.abort?.abort();
        const abort = this.abort = new AbortController();
        try {
            this.volumes = (await listVolumes(this.client, abort.signal)).body;
        } catch (error) {
            if (!abort.signal.aborted) {
                this.notifications?.error("The volumes couldn't be loaded", error);
            }
        }
    }

    override render() {
        const volumes = this.volumes;
        return html`
            <gs-page-header
                heading="Files"
                subtitle=${volumes ? `${volumes.length} ${volumes.length === 1 ? 'volume' : 'volumes'}` : 'Loading…'}
                .icon=${Folder32}
            ></gs-page-header>
            ${volumes ? html`<ul class="gf-volumes">${volumes.map((volume) => html`
                <li>
                    <strong>${volume.name}</strong>
                    <span>${volume.url}</span>
                    ${volume.error ? html`<em>${volume.error}</em>` : volume.mounted ? nothing : html`<em>Not mounted</em>`}
                </li>
            `)}</ul>` : nothing}
        `;
    }
}

if (!customElements.get(FilesTag)) {
    customElements.define(FilesTag, FilesElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [FilesTag]: FilesElement;
    }
}

///////////////////////////////////////////////////////////////////////////////
// MODULE

// FilesApp is what the module needs of the app which includes it: its
// navigation, its router, the filer API's client, its notifications and
// changes, and the element its pages are added to
export interface FilesApp {
    nav: NavController;
    router: Router;
    client: Client;
    root: HTMLElement;
    notifications?: NotificationController;
    changes?: ChangesController;
}

// filesModule adds the Files module to the app: its section, which owns the
// menu bar's Files item, and the sidebar while it's active, of its page,
// which it adds to the root, and routes to. The page's id is page-files, not
// the hash it's routed to, so the browser doesn't scroll to it.
export function filesModule(app: FilesApp, section = 'Files'): FilesElement {
    const page = document.createElement('section');
    page.id = 'page-files';
    page.className = 'page';
    page.hidden = true;

    const element = document.createElement(FilesTag);
    element.client = app.client;
    element.notifications = app.notifications;
    element.changes = app.changes;
    page.append(element);
    app.root.append(page);

    app.nav.setSection(section, [{ id: 'files', label: 'Files', href: '#files' }]);
    app.router.add('files', page);
    return element;
}
