// The Volumes module: the sidebar's Volumes, of the Volumes page, with the
// volumes beneath it, each of which links to its page, which are added and
// removed as the volumes are. A volume's page has a tree of its folders,
// beside the objects of the folder which is chosen,
// which is in the location, as #volume/<name>/<path>, each of which scrolls
// on its own.

import { LitElement, html, nothing, type PropertyValues, type TemplateResult } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import type { Client } from '@mutablelogic/go-server/api';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import type { NavController, NavPage } from '@mutablelogic/go-server/nav';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import type { Route, Router } from '@mutablelogic/go-server/router';
import VolumesIcon from '@carbon/icons/es/data--base/32.js';
import FolderIcon from '@carbon/icons/es/folder/32.js';
import { ObjectChangeSchema } from '../filer/schema.ts';
import { FolderController } from '../folder/controller.ts';
import { baseName, objectTitle, volumeLabel, volumeTitle, type Backend, type Volume } from '../filer/schema.ts';
import { TreeController } from '../tree/controller.ts';
import type { TreeSelection } from '../tree/view.ts';
import { VolumeFilters, VolumesController } from './controller.ts';
import '@mutablelogic/go-server/page/view';
import '../tree/view.ts';
import '../folder/view.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The tags of the Volumes page, and of a volume's page, and their routes,
// such as #volumes, and #volume/media/Music
export const VolumesTag = 'gf-volumes';
export const VolumeTag = 'gf-volume';
export const VolumesRoute = 'volumes';
export const VolumeRoute = 'volume';

///////////////////////////////////////////////////////////////////////////////
// VOLUMES PAGE

// VolumesElement is the Volumes page, of the volumes, and their state,
// which is where what's indexing will be
export class VolumesElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        volumes: { state: true },
    };

    // controller keeps the volumes
    declare controller?: VolumesController;
    declare private volumes?: readonly Volume[];
    private unsubscribe?: () => void;

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
    }

    protected override willUpdate(changed: PropertyValues<this>) {
        if (changed.has('controller') && this.isConnected) {
            this.watch();
        }
    }

    private watch() {
        this.unsubscribe?.();
        this.volumes = this.controller?.volumes;
        this.unsubscribe = this.controller?.subscribe((volumes) => { this.volumes = volumes; });
    }

    override render() {
        const volumes = this.volumes;
        return html`
            <gs-page-header heading="Volumes"
                subtitle=${volumes ? `${volumes.length} ${volumes.length === 1 ? 'volume' : 'volumes'}` : 'Loading…'}
                .icon=${VolumesIcon}></gs-page-header>
            ${volumes ? html`<ul class="gf-volumes">${repeat(volumes, (volume) => volume.name, (volume) => html`
                <li>
                    <a href=${volumeHref(volume.name)} title=${volumeTitle(volume, this.controller?.backends)}>${volumeLabel(volume)}</a>
                    <span title=${volume.url}>${volume.url}</span>
                    ${volumeState(volume)}
                </li>
            `)}</ul>` : nothing}
        `;
    }
}

///////////////////////////////////////////////////////////////////////////////
// VOLUME PAGE

// VolumeElement is a volume's page: a tree of its folders, beside the folder
// which is chosen, or why it isn't mounted
export class VolumeElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        tree: { attribute: false },
        folder: { attribute: false },
        changes: { attribute: false },
        name: { type: String },
        path: { type: String },
        volume: { state: true },
    };

    // controller keeps the volumes, tree their folders, folder the objects
    // of the folder which is chosen, and changes are followed, so they're
    // loaded again when they change
    declare controller?: VolumesController;
    declare tree?: TreeController;
    declare folder?: FolderController;
    declare changes?: ChangesController;

    // name is the volume's, and path the folder's which is chosen
    declare name?: string;
    declare path?: string;

    declare private volume?: Volume;
    private unsubscribe: (() => void)[] = [];

    protected override createRenderRoot() {
        return this;
    }

    override connectedCallback() {
        super.connectedCallback();
        this.watch();
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.unwatch();
    }

    protected override willUpdate(changed: PropertyValues<this>) {
        if ((changed.has('controller') || changed.has('tree') || changed.has('folder') || changed.has('changes')) && this.isConnected) {
            this.watch();
        }
        if (changed.has('controller') || changed.has('name')) {
            this.volume = this.name ? this.controller?.volume(this.name) : undefined;
        }
        if ((changed.has('name') || changed.has('path')) && this.name && this.path) {
            void this.reveal();
        }
        if (changed.has('name') || changed.has('path') || (changed as Map<string, unknown>).has('volume')) {
            this.openFolder();
        }
    }

    // watch follows the volumes, and the tree, whose folders' titles are
    // shown, and forgets the folders of a volume which isn't mounted, and
    // reveals the folder which is chosen again, once it's mounted again,
    // such as after the server starts
    private watch() {
        this.unwatch();
        if (this.controller) {
            this.unsubscribe.push(this.controller.subscribe((volumes) => {
                const wasMounted = this.volume?.mounted;
                this.volume = volumes.find((volume) => volume.name === this.name);
                for (const volume of volumes) {
                    if (!volume.mounted) {
                        this.tree?.forgetVolume(volume.name);
                    }
                }
                if (this.volume?.mounted && !wasMounted && this.name && this.path) {
                    void this.reveal();
                }
            }));
        }
        if (this.tree) {
            this.unsubscribe.push(this.tree.subscribe(() => this.requestUpdate()));
        }
        const folder = this.folder;
        if (folder && this.changes) {
            this.unsubscribe.push(this.changes.subscribe({ schema: ObjectChangeSchema }, (change) => folder.change(change)));
        }
    }

    // reveal shows the folder which is chosen in the tree, and scrolls to it
    private async reveal() {
        const name = this.name;
        const path = this.path;
        if (!name || !path || !this.tree) {
            return;
        }
        await this.tree.reveal(name, path);
        await this.updateComplete;
        const tree = this.querySelector('gf-file-tree');
        await tree?.updateComplete;
        if (name === this.name && path === this.path) {
            tree?.querySelector(`cds-tree-node[data-path="${CSS.escape(path)}"]`)?.scrollIntoView({ block: 'nearest' });
        }
    }

    // openFolder shows the objects of the folder which is chosen, of a
    // volume which is mounted
    private openFolder() {
        if (this.name && this.path && this.volume?.mounted) {
            void this.folder?.open(this.name, this.path);
        } else {
            this.folder?.close();
        }
    }

    private unwatch() {
        for (const fn of this.unsubscribe.splice(0)) {
            fn();
        }
    }

    override render() {
        const name = this.name;
        if (!name) {
            return nothing;
        }
        const path = this.path || '/';
        const volume = this.volume;
        const folder = path !== '/' ? this.tree?.folder(name, path) : undefined;
        // The tree is only shown when the volume's root has folders, once
        // they're loaded, or couldn't be, which it says
        const roots = this.tree?.folders(name, '/');
        const hasFolders = roots?.state === 'error' || (roots?.state === 'loaded' && roots.folders.length > 0);
        const header = html`
            <gs-page-header
                heading=${folder ? objectTitle(folder) : baseName(path) || (volume ? volumeLabel(volume) : name)}
                subtitle=${`${name}${path === '/' ? '' : path}`}
                .icon=${path === '/' ? VolumesIcon : FolderIcon}></gs-page-header>
        `;
        if (!volume) {
            return html`${header}<p class="gf-empty">${this.controller?.volumes ? "This volume doesn't exist" : 'Loading…'}</p>`;
        } else if (!volume.mounted) {
            return html`${header}<p class="gf-empty">${volume.error || "This volume isn't mounted"}</p>`;
        }
        return html`
            ${header}
            ${volume.error ? html`<p class="gf-volume-error">${volume.error}</p>` : nothing}
            <div class="gf-volume">
                <gf-file-tree class="gf-volume-tree" ?hidden=${!hasFolders} .controller=${this.tree} .changes=${this.changes} volume=${name}
                    .selected=${{ volume: name, path }}
                    @gf-file-tree-selected=${(event: CustomEvent<TreeSelection>) => this.chosen(event.detail)}></gf-file-tree>
                <gf-file-view class="gf-volume-view" .controller=${this.folder}
                    empty=${hasFolders ? 'Select a folder from the left' : 'No objects'}></gf-file-view>
            </div>
        `;
    }

    // chosen chooses the folder which is chosen in the tree, and adds it to
    // the history, as its route, without routing to it again
    private chosen(selection: TreeSelection) {
        this.path = selection.path;
        const hash = folderHref(selection.volume, selection.path);
        if (location.hash !== hash) {
            history.pushState(null, '', hash);
        }
    }
}

for (const [tag, element] of [[VolumesTag, VolumesElement], [VolumeTag, VolumeElement]] as const) {
    if (!customElements.get(tag)) {
        customElements.define(tag, element);
    }
}

declare global {
    interface HTMLElementTagNameMap {
        [VolumesTag]: VolumesElement;
        [VolumeTag]: VolumeElement;
    }
}

///////////////////////////////////////////////////////////////////////////////
// MODULE

// VolumesApp is what the module needs of the app which includes it: its
// navigation, its router, the filer API's client, its notifications and
// changes, and the element its pages are added to
export interface VolumesApp {
    nav: NavController;
    router: Router;
    client: Client;
    root: HTMLElement;
    notifications?: NotificationController;
    changes?: ChangesController;
}

// volumesModule adds the Volumes module to the app: its section, which owns
// the sidebar while it's active, of the Volumes page, and each volume's,
// beneath it, which are added and removed as the volumes are, and its two
// pages, which it adds to the root, and routes to. The pages' ids aren't
// the hashes they're routed to, so the browser doesn't scroll to them.
// Returns the controller of the volumes.
export function volumesModule(app: VolumesApp, section = 'Volumes'): VolumesController {
    const controller = new VolumesController(app.client).onError((title, error) => app.notifications?.error(title, error));
    const tree = new TreeController(app.client).onError((title, error) => app.notifications?.error(title, error));

    // The pages
    const volumesPage = page('page-volumes', app.root);
    const volumesElement = document.createElement(VolumesTag);
    volumesElement.controller = controller;
    volumesPage.append(volumesElement);
    const volumePage = page('page-volume', app.root);
    const volumeElement = document.createElement(VolumeTag);
    volumeElement.controller = controller;
    volumeElement.tree = tree;
    volumeElement.folder = new FolderController(app.client).onError((title, error) => app.notifications?.error(title, error));
    volumeElement.changes = app.changes;
    volumePage.append(volumeElement);

    // The sidebar, of the volumes
    const setPages = (volumes: readonly Volume[]) => app.nav.setSection(section, navPages(volumes, controller.backends));
    setPages([]);
    controller.subscribe(setPages);

    // The routes: a volume's is its name, and the path of its folder
    app.router.add(VolumesRoute, volumesPage);
    app.router.add(VolumeRoute, volumePage, (route: Route) => {
        const [name, ...elements] = route.params;
        volumeElement.name = name;
        volumeElement.path = '/' + elements.filter((e) => e).join('/');
    });

    // The volumes are loaded at once, and again when they change, or
    // changes may have been missed
    void controller.reload();
    app.changes?.subscribe(VolumeFilters, (change) => controller.change(change));
    return controller;
}

// navPage returns the id of the sidebar's page of a route, which is a
// volume's, of its name, for a volume's route
export function navPage(route: Route): string {
    return route.page === VolumeRoute && route.params[0] ? `${VolumeRoute}/${route.params[0]}` : route.page;
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// navPages returns the sidebar's pages: Volumes, and each volume beneath it,
// of its label, which is cut short, when it's long, and its title
function navPages(volumes: readonly Volume[], backends?: readonly Backend[]): NavPage[] {
    return [
        { id: VolumesRoute, label: 'Volumes', href: '#' + VolumesRoute },
        ...volumes.map((volume) => ({
            id: `${VolumeRoute}/${volume.name}`, label: volumeLabel(volume), title: volumeTitle(volume, backends),
            href: volumeHref(volume.name), parent: VolumesRoute,
        })),
    ];
}

// page returns a page, which is hidden until it's routed to, in the root
function page(id: string, root: HTMLElement): HTMLElement {
    const element = document.createElement('section');
    element.id = id;
    element.className = 'page';
    element.hidden = true;
    root.append(element);
    return element;
}

// volumeHref returns the link of a volume's page
function volumeHref(name: string): string {
    return '#' + VolumeRoute + '/' + encodeURIComponent(name);
}

// folderHref returns the link of a folder of a volume
function folderHref(name: string, path: string): string {
    return '#' + [VolumeRoute, name, ...path.split('/').filter((e) => e)].map(encodeURIComponent).join('/');
}

// volumeState returns why a volume isn't mounted, or its error, or nothing
function volumeState(volume: Volume): TemplateResult | typeof nothing {
    if (volume.error) {
        return html`<em>${volume.error}</em>`;
    }
    return volume.mounted ? nothing : html`<em>Not mounted</em>`;
}
