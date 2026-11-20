// The gf-file-tree element is a tree of a volume's folders, whose roots are
// the folders of the volume's root, which are loaded as they're expanded,
// a page at a time, as they're scrolled to, and kept up to date with the
// changes. A folder which is clicked is selected, and expanded, or
// collapsed, and the element fires gf-file-tree-selected, with its volume
// and path. A volume without folders has no tree.

import { LitElement, html, nothing, type PropertyValues, type TemplateResult } from 'lit';
import { keyed } from 'lit/directives/keyed.js';
import { repeat } from 'lit/directives/repeat.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import FolderIcon from '@carbon/icons/es/folder/16.js';
import { ObjectChangeSchema, objectTitle, subfolderCount, type FilerObject } from '../filer/schema.ts';
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
    folder: iconLoader(FolderIcon, { slot: 'icon' }),
};

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class FileTreeElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        changes: { attribute: false },
        volume: { type: String },
        selected: { attribute: false },
        version: { state: true },
    };

    // controller keeps the volumes, and their folders, and changes are
    // given to it, so they're loaded again when they change
    declare controller?: TreeController;
    declare changes?: ChangesController;

    // volume is the name of the volume whose folders are shown
    declare volume?: string;

    // selected is the folder which is selected
    declare selected?: TreeSelection;

    // version changes when the controller's tree does, so it's rendered
    declare private version: number;

    private unsubscribe: (() => void)[] = [];
    private observer?: IntersectionObserver;

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
        this.observer?.disconnect();
        this.observer = undefined;
    }

    // Once it's rendered, the labels of the nodes are as wide as the tree
    // (see labelSheet), and more folders are loaded when the node which says
    // they're loading is scrolled to
    protected override updated() {
        for (const node of this.querySelectorAll('cds-tree-node')) {
            const root = node.shadowRoot;
            if (root && !root.adoptedStyleSheets.includes(labelSheet)) {
                root.adoptedStyleSheets = [...root.adoptedStyleSheets, labelSheet];
            }
        }
        this.observer?.disconnect();
        const more = this.querySelectorAll<HTMLElement>('.gf-tree-more');
        if (more.length === 0) {
            this.observer = undefined;
            return;
        }
        this.observer = new IntersectionObserver((entries) => {
            for (const entry of entries) {
                const { moreVolume: volume, morePath: path } = (entry.target as HTMLElement).dataset;
                if (entry.isIntersecting && volume && path) {
                    void this.controller?.loadMore(volume, path);
                }
            }
        }, { rootMargin: '200px' });
        more.forEach((node) => this.observer!.observe(node));
    }

    protected override willUpdate(changed: PropertyValues<this>) {
        if (this.isConnected && (changed.has('controller') || changed.has('changes') || changed.has('volume'))) {
            this.watch();
        }
    }

    // watch follows the controller, and gives it the changes of the
    // volume's objects. Its root is expanded at once, rather than once the
    // changes are watched, so it's shown when they can't be.
    private watch() {
        this.unwatch();
        const controller = this.controller;
        const volume = this.volume;
        if (!controller || !volume) {
            return;
        }
        this.unsubscribe.push(controller.subscribe(() => this.version++));
        void controller.expand(volume, '/');
        if (this.changes) {
            this.unsubscribe.push(this.changes.subscribe({ schema: ObjectChangeSchema, table: volume }, (change) => controller.change(change)));
        }
    }

    private unwatch() {
        for (const fn of this.unsubscribe.splice(0)) {
            fn();
        }
    }

    // Nodes are chosen, not text, which Carbon only shows for nodes with
    // links
    protected override render() {
        const volume = this.volume;
        const roots = volume ? this.controller?.folders(volume, '/') : undefined;
        if (!volume || !roots) {
            return nothing;
        } else if (roots.state !== 'loaded') {
            return html`<p class="gf-empty">${roots.state === 'error' ? "The folders couldn't be loaded" : 'Loading…'}</p>`;
        } else if (roots.folders.length === 0) {
            return nothing;
        }
        return html`
            <cds-tree-view label=${`Folders of ${volume}`} hide-label size="sm"
                @click=${(event: Event) => this.clicked(event)}
                @cds-tree-node-toggled=${(event: CustomEvent<{ expanded: boolean }>) => this.toggled(event)}
                @cds-tree-node-selected=${(event: Event) => this.chosen(event)}>
                ${repeat(roots.folders, (folder) => folder.path, (folder) => this.nodeView(volume, folder.path, objectTitle(folder), Icons.folder, folder))}
                ${roots.more ? moreView(volume, '/') : nothing}
            </cds-tree-view>
        `;
    }

    // nodeView is a folder's node, and, when it's expanded, its subfolders'.
    // One which may have subfolders, but whose aren't loaded, has a node
    // which says they're loading, so it can be expanded; one which has none
    // has no caret. Carbon sets a node's indent once it has children, but
    // not again when it hasn't any more, so a node is another when it
    // becomes one without subfolders, or with them.
    private nodeView(volume: string, path: string, label: string, icon: unknown, folder?: FilerObject): unknown {
        const controller = this.controller!;
        const folders = controller.folders(volume, path);
        const loaded = folders?.state === 'loaded' ? folders : undefined;
        const leaf = (folder !== undefined && subfolderCount(folder) === 0) || loaded?.folders.length === 0;
        const selected = this.selected?.volume === volume && this.selected.path === path;
        return keyed(leaf, html`
            <cds-tree-node label=${label} data-volume=${volume} data-path=${path} ?data-leaf=${leaf} .isExpanded=${!leaf && controller.isExpanded(volume, path)} .selected=${selected}>
                ${icon}
                ${leaf ? nothing : loaded ? html`
                    ${repeat(loaded.folders, (child) => child.path, (child) => this.nodeView(volume, child.path, objectTitle(child), Icons.folder, child))}
                    ${loaded.more ? moreView(volume, path) : nothing}
                ` : html`<cds-tree-node label=${folders?.state === 'error' ? "They couldn't be loaded" : 'Loading…'} disabled></cds-tree-node>`}
            </cds-tree-node>
        `);
    }

    // toggled loads a folder's subfolders when it's expanded, the first time.
    // Carbon's nodes say they're toggled when they're first rendered, too,
    // which, as they're as the tree has them, are ignored.
    private toggled(event: CustomEvent<{ expanded: boolean }>) {
        const node = nodeOf(event);
        if (!node || !this.controller || this.controller.isExpanded(node.volume, node.path) === event.detail.expanded) {
            return;
        } else if (event.detail.expanded) {
            void this.controller.expand(node.volume, node.path);
        } else {
            this.controller.collapse(node.volume, node.path);
        }
    }

    // clicked selects a folder which is clicked, other than its caret, which
    // toggles it, and expands it, or collapses it, unless it has no
    // folders. A folder which is selected isn't selected again, of Carbon's
    // tree, so the click is followed, rather than the selection.
    private clicked(event: Event) {
        const target = event.composedPath().find((t): t is HTMLElement => t instanceof HTMLElement && t.tagName === 'CDS-TREE-NODE');
        const node = nodeOf(event);
        if (!target || !node || target.hasAttribute('disabled') || !this.controller) {
            return;
        }
        this.chosen(event);
        if (!target.hasAttribute('data-leaf')) {
            if (this.controller.isExpanded(node.volume, node.path)) {
                this.controller.collapse(node.volume, node.path);
            } else {
                void this.controller.expand(node.volume, node.path);
            }
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

// labelSheet makes the labels of the nodes as wide as the tree: Carbon's are
// 100% wide, which a node with children, which is a list item, indents, so
// its label, which it moves back, of a negative margin, falls short of the
// tree's right, and so does its selection, which a leaf's, which is flex,
// doesn't
const labelSheet = new CSSStyleSheet();
labelSheet.replaceSync('.cds--tree-node__label { inline-size: auto; }');

// moreView is the node which says more of a folder's folders are loading,
// which loads them, once it's scrolled to
function moreView(volume: string, path: string): TemplateResult {
    return html`<cds-tree-node class="gf-tree-more" label="Loading more…" disabled data-more-volume=${volume} data-more-path=${path}></cds-tree-node>`;
}

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
