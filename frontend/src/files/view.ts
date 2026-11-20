// The Files module: a page of a tree of the volumes, and their folders, and
// the folder which is chosen, which is in the location, as
// #files/<volume>/<path>, where its file view will be

import { LitElement, html, nothing, type PropertyValues } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import type { NavController } from '@mutablelogic/go-server/nav';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import type { Route, Router } from '@mutablelogic/go-server/router';
import FolderIcon from '@carbon/icons/es/folder/32.js';
import { baseName, objectTitle } from '../filer/schema.ts';
import { TreeController } from '../tree/controller.ts';
import type { TreeSelection } from '../tree/view.ts';
import '@mutablelogic/go-server/page/view';
import '../tree/view.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FilesTag is the tag of the Files page, and FilesRoute its route
export const FilesTag = 'gf-files';
export const FilesRoute = 'files';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FilesElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        notifications: { attribute: false },
        changes: { attribute: false },
        selected: { attribute: false },
    };

    // client is the filer API's client, notifications show its errors, and
    // changes are followed, so what's shown is loaded again when it changes
    declare client?: Client;
    declare notifications?: NotificationController;
    declare changes?: ChangesController;

    // selected is the folder which is chosen
    declare selected?: TreeSelection;

    private tree?: TreeController;
    private unsubscribe?: () => void;

    // The page is in the light DOM, so the app's styles apply
    protected override createRenderRoot() {
        return this;
    }

    // The tree is the client's, and the page is rendered again when it
    // changes, as the title of the folder which is chosen is the tree's
    protected override willUpdate(changed: PropertyValues<this>) {
        if (changed.has('client') || changed.has('notifications')) {
            this.unsubscribe?.();
            this.tree = this.client ? new TreeController(this.client).onError((title, error) => this.notifications?.error(title, error)) : undefined;
            this.unsubscribe = this.tree?.subscribe(() => this.requestUpdate());
            if (this.selected) {
                void this.tree?.reveal(this.selected.volume, this.selected.path);
            }
        }
    }

    // route selects the folder of a route's parameters, its volume, and the
    // elements of its path
    route(route: Route) {
        const [volume, ...elements] = route.params;
        const selected = volume ? { volume, path: '/' + elements.filter((e) => e).join('/') } : undefined;
        if (selected?.volume !== this.selected?.volume || selected?.path !== this.selected?.path) {
            this.selected = selected;
            if (selected) {
                void this.tree?.reveal(selected.volume, selected.path);
            }
        }
    }

    override render() {
        const selected = this.selected;
        const folder = selected && selected.path !== '/' ? this.tree?.folder(selected.volume, selected.path) : undefined;
        return html`
            <gs-page-header
                heading=${selected ? (folder ? objectTitle(folder) : baseName(selected.path) || selected.volume) : 'Files'}
                subtitle=${selected ? `${selected.volume}${selected.path === '/' ? '' : selected.path}` : 'Choose a folder'}
                .icon=${FolderIcon}
            ></gs-page-header>
            <div class="gf-files">
                <gf-file-tree class="gf-files-tree" .controller=${this.tree} .changes=${this.changes} .selected=${selected}
                    @gf-file-tree-selected=${(event: CustomEvent<TreeSelection>) => this.chosen(event.detail)}></gf-file-tree>
                <div class="gf-files-view">${selected ? nothing : html`<p class="gf-files-empty">Choose a volume, or a folder, in the tree</p>`}</div>
            </div>
        `;
    }

    // chosen selects the folder which is chosen in the tree, and adds it to
    // the history, as its route, without routing to it again
    private chosen(selection: TreeSelection) {
        this.selected = selection;
        const hash = '#' + [FilesRoute, selection.volume, ...selection.path.split('/').filter((e) => e)].map(encodeURIComponent).join('/');
        if (location.hash !== hash) {
            history.pushState(null, '', hash);
        }
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
// which it adds to the root, and routes to, with the folder which is chosen.
// The page's id is page-files, not the hash it's routed to, so the browser
// doesn't scroll to it.
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

    app.nav.setSection(section, [{ id: FilesRoute, label: 'Files', href: '#' + FilesRoute }]);
    app.router.add(FilesRoute, page, (route) => element.route(route));
    return element;
}
