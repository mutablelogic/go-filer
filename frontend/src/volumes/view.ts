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
import Edit16 from '@carbon/icons/es/edit/16.js';
import FolderIcon16 from '@carbon/icons/es/folder/16.js';
import ObjectStorageIcon from '@carbon/icons/es/object-storage/16.js';
import RssIcon from '@carbon/icons/es/rss/16.js';
import CalendarIcon from '@carbon/icons/es/calendar/16.js';
import ContactsIcon from '@carbon/icons/es/user--multiple/16.js';
import CloudIcon from '@carbon/icons/es/cloud/16.js';
import TaskIcon from '@carbon/icons/es/task/16.js';
import DataBaseIcon from '@carbon/icons/es/data--base/16.js';
import { iconLoader } from '@carbon/web-components/es/globals/internal/icon-loader.js';
import { ObjectChangeSchema } from '../filer/schema.ts';
import { FolderController } from '../folder/controller.ts';
import { baseName, objectTitle, volumeBackend, volumeIndexFailure, volumeIndexing, volumeKinds, volumeLabel, volumeNoun, volumeObjects, volumeObjectsUnknown, volumeScheme, volumeStatus, volumeTitle, type Backend, type Volume } from '../filer/schema.ts';
import { TreeController } from '../tree/controller.ts';
import type { TreeSelection } from '../tree/view.ts';
import { VolumeFilters, VolumesController } from './controller.ts';
import '@mutablelogic/go-server/page/view';
import '../tree/view.ts';
import '../folder/view.ts';
import '@carbon/web-components/es/components/tile/clickable-tile.js';
import '@carbon/web-components/es/components/tag/tag.js';
import '@carbon/web-components/es/components/tabs/index.js';
import '../chart/meter.ts';
import '@carbon/web-components/es/components/icon-button/index.js';
import './panel.ts';
import './add.ts';
import { VolumeFields } from './create.ts';
import type { PageAction } from '@mutablelogic/go-server/page';
import { alertView } from '../alert/view.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The tags of the Volumes page, and of a volume's page, and their routes,
// such as #volumes, and #volume/media/Music
export const VolumesTag = 'gf-volumes';

// StatsTab is a tab of the volumes' stats, of which Objects is chosen first
type StatsTab = 'objects' | 'indexing';
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
        notifications: { attribute: false },
        volumes: { state: true },
        tab: { state: true },
    };

    // controller keeps the volumes, notifications are told why a volume
    // can't be changed, of its panel, and tab is the tab of their stats which is shown,
    // beneath their tiles
    declare controller?: VolumesController;
    declare notifications?: NotificationController;
    declare private volumes?: readonly Volume[];
    declare private tab: StatsTab;

    constructor() {
        super();
        this.tab = 'objects';
    }
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
                .icon=${VolumesIcon} .actions=${this.addActions()}></gs-page-header>
            ${volumes ? this.failuresView(volumes) : nothing}
            ${volumes ? html`<div class="gf-volume-tiles">${repeat(volumes, (volume) => volume.name, (volume) => this.tileView(volume))}</div>` : nothing}
            ${volumes?.length ? this.statsView(volumes) : nothing}
            <gf-volume-panel .controller=${this.controller} .notifications=${this.notifications}></gf-volume-panel>
            <gf-volume-add .controller=${this.controller} .notifications=${this.notifications}></gf-volume-add>
        `;
    }

    // failuresView is an alert of each volume whose index failed, with why,
    // which is there until it's indexed again, as the volumes change
    private failuresView(volumes: readonly Volume[]): TemplateResult {
        return html`${repeat(volumes.filter((v) => volumeIndexFailure(v)), (v) => v.name, (v) =>
            alertView('error', `Indexing ${volumeLabel(v)} failed`, volumeIndexFailure(v)))}`;
    }

    // addActions are Add Volume, a menu of the backends, each of which opens
    // the panel which adds a volume of it, of those it can add
    private addActions(): PageAction[] {
        const backends = this.controller?.backends ?? [];
        if (backends.length === 0) {
            return [];
        }
        return [{
            label: 'Add Volume',
            menu: backends.map((backend) => ({
                label: backend.description || backend.scheme,
                icon: BackendIcons.get(backend.scheme) ?? DataBaseIcon,
                disabled: !VolumeFields.has(backend.scheme),
                run: () => this.querySelector('gf-volume-add')?.open(backend),
            })),
        }];
    }

    // edit opens the panel of a volume, of its View tab
    private edit(volume: Volume) {
        this.querySelector('gf-volume-panel')?.open(volume, 'view');
    }

    // statsView is the tabs of the volumes' stats, of which the one which is
    // chosen is shown: Indexing, and Objects, which is to come
    private statsView(volumes: readonly Volume[]): TemplateResult {
        const tab = this.tab;
        return html`
            <section class="gf-volume-stats">
                <cds-tabs value=${tab} @cds-tabs-selected=${(event: CustomEvent<{ item: HTMLElement & { value: string } }>) => { this.tab = event.detail.item.value as StatsTab; }}>
                    <cds-tab value="objects">Objects</cds-tab>
                    <cds-tab value="indexing">Indexing</cds-tab>
                </cds-tabs>
                ${this.sectionsView(volumes, tab, tab === 'objects' ? kindsMeter : indexingMeter)}
            </section>
        `;
    }

    // sectionsView is a section of each volume, of a tab: its label, as a
    // heading, and its status, when it isn't idle, with why, when it's
    // pointed at, and how many folders and objects it has, as tags, beneath
    // it, and the tab's meter of it, whose sections are of the tab, so a
    // meter isn't of another's
    private sectionsView(volumes: readonly Volume[], tab: StatsTab, meter: (volume: Volume) => TemplateResult | typeof nothing): TemplateResult {
        return html`<div class="gf-indexing">${repeat(volumes, (volume) => `${tab}:${volume.name}`, (volume) => {
            const objects = volumeObjects(volume);
            const folders = volume.index?.folders;
            return html`
                <section class="gf-indexing-volume">
                    <h3><a href=${volumeHref(volume.name)} title=${volumeTitle(volume, this.controller?.backends)}>${volumeLabel(volume)}</a></h3>
                    <div class="gf-indexing-tags">
                        ${statusTag(volume)}
                        ${folders !== undefined ? html`<cds-tag size="sm" type="cool-gray">${folders.toLocaleString()} ${folders === 1 ? 'folder' : 'folders'}</cds-tag>` : nothing}
                        ${objects !== undefined ? html`<cds-tag size="sm" type="cool-gray">${objects.toLocaleString()} ${volumeNoun(volume, objects)}</cds-tag>` : nothing}
                    </div>
                    ${meter(volume)}
                </section>
            `;
        })}</div>`;
    }

    // tileView is a volume's tile, which goes to its page: its label, and
    // its name, its backend, as a tag, and how many objects it has, with a
    // button, at its bottom right, beside it, rather than in it, which edits
    // it
    private tileView(volume: Volume): TemplateResult {
        const backends = this.controller?.backends;
        const backend = volumeBackend(volume, backends);
        return html`
            <div class="gf-volume-tile-wrap">
                <cds-clickable-tile class="gf-volume-tile" href=${volumeHref(volume.name)} title=${volumeTitle(volume, backends)}>
                    ${backend ? html`<cds-tag size="sm" type=${tagType(volume, backends)}>${backend}</cds-tag>` : nothing}
                    <h3 class="gf-volume-tile-label">${volumeLabel(volume)}</h3>
                    <p class="gf-volume-tile-name">${volume.name}</p>
                    ${tileCount(volume)}
                </cds-clickable-tile>
                <cds-icon-button class="gf-volume-tile-edit" kind="ghost" size="sm" align="top-end"
                    @click=${() => this.edit(volume)}>
                    ${EditIcon}
                    <span slot="tooltip-content">Edit</span>
                </cds-icon-button>
            </div>
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
        notifications: { attribute: false },
        name: { type: String },
        path: { type: String },
        volume: { state: true },
    };

    // controller keeps the volumes, tree their folders, folder the objects
    // of the folder which is chosen, and changes are followed, so they're
    // loaded again when they change
    declare controller?: VolumesController;
    declare tree?: TreeController;
    declare notifications?: NotificationController;
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
                .icon=${path === '/' ? VolumesIcon : FolderIcon} .actions=${volume ? this.editActions(volume) : []}>
                ${volume ? html`<div class="gf-volume-header-status">${statusTag(volume, 'md')}</div>` : nothing}
            </gs-page-header>
            <gf-volume-panel .controller=${this.controller} .notifications=${this.notifications}></gf-volume-panel>
        `;
        if (!volume) {
            return html`${header}<p class="gf-empty">${this.controller?.volumes ? "This volume doesn't exist" : 'Loading…'}</p>`;
        } else if (!volume.mounted) {
            // One which is unmounted is, of its own, so it isn't an error,
            // which its tag says, too
            return volumeStatus(volume) === 'Unmounted'
                ? html`${header}<p class="gf-empty">This volume is unmounted, so its files aren't shown until it's mounted again</p>`
                : html`${header}${alertView('error', "This volume isn't mounted", volume.error)}`;
        }
        return html`
            ${header}
            ${volume.error ? alertView('warning', 'This volume is offline, so what it shows is what was last indexed', volume.error) : nothing}
            <div class="gf-volume">
                <gf-file-tree class="gf-volume-tree" ?hidden=${!hasFolders} .controller=${this.tree} .changes=${this.changes} volume=${name}
                    .selected=${{ volume: name, path }}
                    @gf-file-tree-selected=${(event: CustomEvent<TreeSelection>) => this.chosen(event.detail)}></gf-file-tree>
                <gf-file-view class="gf-volume-view" .controller=${this.folder}
                    empty=${hasFolders ? 'Select a folder from the left' : 'No objects'}></gf-file-view>
            </div>
        `;
    }

    // editActions are Edit Volume, which opens the volume's panel, of its
    // View tab, as its tile's pencil does
    private editActions(volume: Volume): PageAction[] {
        return [{ label: 'Edit Volume', kind: 'secondary', icon: Edit16, run: () => this.querySelector('gf-volume-panel')?.open(volume, 'view') }];
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
    volumesElement.notifications = app.notifications;
    volumesPage.append(volumesElement);
    const volumePage = page('page-volume', app.root);
    const volumeElement = document.createElement(VolumeTag);
    volumeElement.controller = controller;
    volumeElement.notifications = app.notifications;
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
// of its label, which is cut short, when it's long, and its title, which is
// disabled while it's unmounted, so it's dimmed, and isn't linked to
function navPages(volumes: readonly Volume[], backends?: readonly Backend[]): NavPage[] {
    return [
        { id: VolumesRoute, label: 'Volumes', href: '#' + VolumesRoute },
        ...volumes.map((volume) => {
            const unmounted = volumeStatus(volume) === 'Unmounted';
            return {
                id: `${VolumeRoute}/${volume.name}`, label: volumeLabel(volume),
                title: volumeTitle(volume, backends) + (unmounted ? '\nUnmounted' : ''),
                href: volumeHref(volume.name), parent: VolumesRoute, disabled: unmounted,
            };
        }),
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

// IndexingColors are the colours of a meter's objects which are indexed,
// which failed, and which aren't indexed, of Carbon's data visualisation
// palette: purple 70 and teal 50, its pairing of two, and magenta 70
const IndexingColors = { Indexed: '#6929c4', Failed: '#9f1853', Unindexed: '#009d9a' };

// kindsMeter is a meter of a volume's objects of each kind, of those which
// have the most, the rest of which are Other, when its kinds are known
function kindsMeter(volume: Volume): TemplateResult | typeof nothing {
    const kinds = volumeKinds(volume);
    if (!kinds) {
        return nothing;
    }
    const total = kinds.reduce((sum, kind) => sum + kind.value, 0);
    return html`
        <gf-meter .groups=${kinds} total=${total} unit=${volumeNoun(volume, total)}
            label=${`${volumeLabel(volume)}: objects`}></gf-meter>
    `;
}

// indexingMeter is a meter of a volume's objects which are indexed, which
// failed, and which aren't indexed yet, of how many it has, when that's known
function indexingMeter(volume: Volume): TemplateResult | typeof nothing {
    const indexing = volumeIndexing(volume);
    const objects = volumeObjects(volume) ?? 0;
    if (!indexing) {
        return nothing;
    }
    return html`
        <gf-meter .groups=${[
            { group: 'Indexed', value: indexing.indexed },
            { group: 'Failed', value: indexing.failed },
            { group: 'Unindexed', value: indexing.unindexed },
        ]} .colors=${IndexingColors} .descriptions=${indexingDescriptions(volume)} total=${objects} unit=${volumeNoun(volume, objects)}
            label=${`${volumeLabel(volume)}: indexing`}></gf-meter>
    `;
}

// indexingDescriptions returns what a volume's objects which are indexed,
// which failed, and which aren't indexed, are, of their meter's legend, in
// the backend's words, such as files
function indexingDescriptions(volume: Volume): Record<string, string> {
    const noun = volumeNoun(volume, 2);
    const objects = noun.charAt(0).toUpperCase() + noun.slice(1);
    return {
        Indexed: `${objects} which have been added to the index`,
        Failed: `${objects} which couldn't be completely indexed`,
        Unindexed: `${objects} which aren't yet searchable`,
    };
}

// statusTag returns a tag of a volume's status, when it isn't Ready, whose
// title says why, when it's pointed at, or nothing
export function statusTag(volume: Volume, size: 'sm' | 'md' = 'sm'): TemplateResult | typeof nothing {
    const status = volumeStatus(volume);
    if (status === 'Ready') {
        return nothing;
    }
    const error = volume.error || volume.index?.error || volume.index?.metadata?.error;
    return html`<cds-tag size=${size} type=${statusTagType(status)} title=${statusTitle(volume, status, error)}>${status}</cds-tag>`;
}

// statusTagType returns the colour of the tag of a volume's status, which
// isn't idle: blue, while it's scanned, or indexed, red, when indexing it
// failed, or it's offline, and gray, otherwise
function statusTagType(status: string): string {
    switch (status) {
        case 'Scanning':
        case 'Indexing':
            return 'blue';
        case 'Indexing error':
        case 'Offline':
            return 'red';
        default:
            return 'gray';
    }
}

// statusTitle returns why a volume's status is what it is, which its tag
// says when it's pointed at: that it isn't searched, when it isn't
// indexed, that it's unmounted, or why it failed, or why how many objects
// it has isn't known
function statusTitle(volume: Volume, status: string, error?: string): string {
    switch (status) {
        case 'Not indexed':
            return "It isn't indexed, and therefore not searched";
        case 'Unmounted':
            return "It's unmounted, so it isn't watched, or searched, until it's mounted again";
        case 'Not mounted':
            return error || "It hasn't been mounted yet";
    }
    return error || volumeObjectsUnknown(volume);
}

// EditIcon is Carbon's edit icon, of an icon button
const EditIcon = iconLoader(Edit16, { slot: 'icon' });

// BackendIcons are the icons of the backends, of their schemes, of the Add
// Volume menu, of which another's is a database's
const BackendIcons = new Map<string, unknown>([
    ['file', FolderIcon16],
    ['s3', ObjectStorageIcon],
    ['rss', RssIcon],
    ['gcalendar', CalendarIcon],
    ['gcontacts', ContactsIcon],
    ['gdrive', CloudIcon],
    ['gtasks', TaskIcon],
]);

// TagTypes are the colours of the backends' tags, in the order of the
// backends, so each backend has its own, while there are enough
const TagTypes = ['blue', 'teal', 'purple', 'magenta', 'cyan', 'green', 'warm-gray', 'red'];

// tagType returns the colour of the tag of a volume's backend
function tagType(volume: Volume, backends: readonly Backend[] = []): string {
    const i = backends.findIndex((backend) => backend.scheme === volumeScheme(volume));
    return i < 0 ? 'cool-gray' : TagTypes[i % TagTypes.length];
}

// tileCount returns how many objects a volume has, of a tile, with what
// they are beneath, such as events, or "–", when it isn't known, with why, and that it's
// offline, when it is
function tileCount(volume: Volume): TemplateResult {
    if (!volume.mounted) {
        // One which is unmounted, of its own, isn't an error
        const status = volumeStatus(volume);
        return html`<p class="gf-volume-tile-state ${status === 'Unmounted' ? '' : 'gf-volume-tile-error'}">${status}</p>`;
    }
    const objects = volumeObjects(volume);
    const title = objects === undefined
        ? volumeObjectsUnknown(volume)
        : `${objects.toLocaleString()} ${volumeNoun(volume, objects)}`;
    return html`
        <p class="gf-volume-tile-count" title=${title}>
            <strong>${objects === undefined ? '–' : objects.toLocaleString()}</strong>
            ${volumeNoun(volume, objects ?? 0)}
            ${volume.error ? html`<span class="gf-volume-tile-error">Offline</span>` : nothing}
        </p>
    `;
}
