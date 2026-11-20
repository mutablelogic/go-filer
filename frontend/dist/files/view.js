// The Files module: a page of the volumes, which is where the file tree,
// and the file view, of a volume's objects, will be
import { LitElement, html, nothing } from 'lit';
import Folder32 from '@carbon/icons/es/folder/32.js';
import { listVolumes } from "../filer/httpclient.js";
import { StoreChangeSchema, VolumeChangeTable } from "../filer/schema.js";
import '@mutablelogic/go-server/page/view';
///////////////////////////////////////////////////////////////////////////////
// GLOBALS
// FilesTag is the tag of the Files page
export const FilesTag = 'gf-files';
///////////////////////////////////////////////////////////////////////////////
// ELEMENT
export class FilesElement extends LitElement {
    static properties = {
        client: { attribute: false },
        notifications: { attribute: false },
        changes: { attribute: false },
        volumes: { state: true },
    };
    unsubscribe;
    abort;
    // The page is in the light DOM, so the app's styles apply
    createRenderRoot() {
        return this;
    }
    connectedCallback() {
        super.connectedCallback();
        if (this.changes) {
            this.unsubscribe = this.changes.subscribe({ schema: StoreChangeSchema, table: VolumeChangeTable }, () => this.load());
        }
        else {
            this.load();
        }
    }
    disconnectedCallback() {
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
        }
        catch (error) {
            if (!abort.signal.aborted) {
                this.notifications?.error("The volumes couldn't be loaded", error);
            }
        }
    }
    render() {
        const volumes = this.volumes;
        return html `
            <gs-page-header
                heading="Files"
                subtitle=${volumes ? `${volumes.length} ${volumes.length === 1 ? 'volume' : 'volumes'}` : 'Loading…'}
                .icon=${Folder32}
            ></gs-page-header>
            ${volumes ? html `<ul class="gf-volumes">${volumes.map((volume) => html `
                <li>
                    <strong>${volume.name}</strong>
                    <span>${volume.url}</span>
                    ${volume.mounted ? nothing : html `<em>${volume.error || 'Not mounted'}</em>`}
                </li>
            `)}</ul>` : nothing}
        `;
    }
}
if (!customElements.get(FilesTag)) {
    customElements.define(FilesTag, FilesElement);
}
// filesModule adds the Files module to the app: its section, which owns the
// menu bar's Files item, and the sidebar while it's active, of its page,
// which it adds to the root, and routes to. The page's id is page-files, not
// the hash it's routed to, so the browser doesn't scroll to it.
export function filesModule(app, section = 'Files') {
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
//# sourceMappingURL=view.js.map