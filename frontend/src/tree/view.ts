// The gf-file-tree element is a tree of the volumes, and their folders,
// which are loaded as they're expanded, and kept up to date with the
// changes. A folder which is chosen is selected, and the element fires
// gf-file-tree-selected, with its volume and path, which is "/" of a
// volume's root.

import { LitElement, html, nothing, type PropertyValues, type TemplateResult } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import FolderIcon from '@carbon/icons/es/folder/16.js';
import VolumeIcon from '@carbon/icons/es/data--base/16.js';
import { objectTitle, subfolderCount, type FilerObject, type Volume } from '../filer/schema.ts';
import { RegistryChangeSchema, ObjectChangeSchema, StoreChangeSchema, VolumeChangeTable } from '../filer/schema.ts';
import type { TreeController } from './controller.ts';
import '@carbon/web-components/es/components/tree-view/index.js';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// TreeSelection is a folder: its volume, and its path, which is "/" of the
// volume's root
export interface TreeSelection {
    volume: string;
    path: string;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// FileTreeTag is the element's tag, and FileTreeSelectedEvent the event it
// fires, with a TreeSelection, when a folder is chosen
export const FileTreeTag = 'gf-file-tree';
export const FileTreeSelectedEvent = 'gf-file-tree-selected';

const Icons = {
    volume: iconLoader(VolumeIcon, { slot: 'icon' }),
    folder: iconLoader(FolderIcon, { slot: 'icon' }),
};

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileTreeElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        changes: { attribute: false },
        selected: { attribute: false },
        version: { state: true },
    };

    // controller keeps the volumes, and their folders, and changes are
    // given to it, so they're loaded again when they change
    declare controller?: TreeController;
    declare changes?: ChangesController;

    // selected is the folder which is selected
    declare selected?: TreeSelection;

    // version changes when the controller's tree does, so it's rendered
    declare private version: number;

    private unsubscribe: (() => void)[] = [];

    constructor() {
        super();
        this.version = 0;
    }

    // The tree is in the light DOM, so the app's styles apply
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
        if (this.isConnected && (changed.has('controller') || changed.has('changes'))) {
            this.watch();
        }
    }

    // watch follows the controller, and gives it the changes of the volumes
    // and their objects. The volumes are loaded at once, rather than once
    // the changes are watched, so they're shown when they can't be.
    private watch() {
        this.unwatch();
        const controller = this.controller;
        if (!controller) {
            return;
        }
        this.unsubscribe.push(controller.subscribe(() => this.version++));
        void controller.loadVolumes();
        if (this.changes) {
            this.unsubscribe.push(this.changes.subscribe([
                { schema: StoreChangeSchema, table: VolumeChangeTable },
                { schema: RegistryChangeSchema, table: VolumeChangeTable },
                { schema: ObjectChangeSchema },
            ], (change) => controller.change(change)));
        }
    }

    private unwatch() {
        for (const fn of this.unsubscribe.splice(0)) {
            fn();
        }
    }

    protected override render() {
        const volumes = this.controller?.volumes;
        if (!volumes) {
            return html`<p class="gf-tree-empty">Loading…</p>`;
        } else if (volumes.length === 0) {
            return html`<p class="gf-tree-empty">No volumes</p>`;
        }
        // Nodes are chosen, not text, which Carbon only shows for nodes with
        // links
        return html`
            <cds-tree-view label="Folders" hide-label size="sm"
                @cds-tree-node-toggled=${(event: CustomEvent<{ expanded: boolean }>) => this.toggled(event)}
                @cds-tree-node-selected=${(event: Event) => this.chosen(event)}>
                ${repeat(volumes, (volume) => volume.name, (volume) => this.volumeView(volume))}
            </cds-tree-view>
        `;
    }

    // volumeView is a volume's node, which can't be expanded, or chosen,
    // unless it's mounted
    private volumeView(volume: Volume): TemplateResult {
        if (!volume.mounted) {
            return html`
                <cds-tree-node label=${volume.name} title=${volume.error || 'Not mounted'} disabled data-volume=${volume.name} data-path="/">
                    ${Icons.volume}
                </cds-tree-node>
            `;
        }
        return this.nodeView(volume.name, '/', volume.name, Icons.volume);
    }

    // nodeView is a folder's node, and, when it's expanded, its subfolders'.
    // One which may have subfolders, but whose aren't loaded, has a node
    // which says they're loading, so it can be expanded; one which has none
    // has no caret.
    private nodeView(volume: string, path: string, label: string, icon: unknown, folder?: FilerObject): TemplateResult {
        const controller = this.controller!;
        const folders = controller.folders(volume, path);
        const loaded = folders?.state === 'loaded' ? folders : undefined;
        const leaf = (folder !== undefined && subfolderCount(folder) === 0) || loaded?.folders.length === 0;
        const selected = this.selected?.volume === volume && this.selected.path === path;
        return html`
            <cds-tree-node label=${label} data-volume=${volume} data-path=${path} .isExpanded=${!leaf && controller.isExpanded(volume, path)} .selected=${selected}>
                ${icon}
                ${leaf ? nothing : loaded ? html`
                    ${repeat(loaded.folders, (child) => child.path, (child) => this.nodeView(volume, child.path, objectTitle(child), Icons.folder, child))}
                    ${loaded.more ? html`<cds-tree-node label="More folders aren't shown" disabled></cds-tree-node>` : nothing}
                ` : html`<cds-tree-node label=${folders?.state === 'error' ? "They couldn't be loaded" : 'Loading…'} disabled></cds-tree-node>`}
            </cds-tree-node>
        `;
    }

    // toggled loads a folder's subfolders when it's expanded, the first time
    private toggled(event: CustomEvent<{ expanded: boolean }>) {
        const node = nodeOf(event);
        if (!node || !this.controller) {
            return;
        } else if (event.detail.expanded) {
            void this.controller.expand(node.volume, node.path);
        } else {
            this.controller.collapse(node.volume, node.path);
        }
    }

    // chosen selects the folder which is chosen
    private chosen(event: Event) {
        const node = nodeOf(event);
        if (node && (this.selected?.volume !== node.volume || this.selected.path !== node.path)) {
            this.selected = node;
            this.dispatchEvent(new CustomEvent<TreeSelection>(FileTreeSelectedEvent, { detail: node, bubbles: true, composed: true }));
        }
    }
}

if (!customElements.get(FileTreeTag)) {
    customElements.define(FileTreeTag, FileTreeElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [FileTreeTag]: FileTreeElement;
    }
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// nodeOf returns the folder of the node an event is of
function nodeOf(event: Event): TreeSelection | undefined {
    for (const target of event.composedPath()) {
        if (target instanceof HTMLElement && target.tagName === 'CDS-TREE-NODE') {
            const volume = target.dataset.volume;
            return volume ? { volume, path: target.dataset.path || '/' } : undefined;
        }
    }
    return undefined;
}
