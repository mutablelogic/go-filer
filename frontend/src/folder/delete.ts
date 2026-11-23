// The panel which deletes files of a writable volume, once it's confirmed:
// which files, of Carbon's contained list, each of which can be kept,
// rather than deleted, and that they're deleted from the volume's backend,
// such as a folder, or a bucket, which can't be undone. confirm opens it, and
// resolves once they're deleted, or it's closed without them being.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import TrashCan16 from '@carbon/icons/es/trash-can/16.js';
import Close16 from '@carbon/icons/es/close/16.js';
import Document16 from '@carbon/icons/es/document/16.js';
import iconLoader from '@carbon/web-components/es/globals/internal/icon-loader.js';
import '@carbon/web-components/es/components/contained-list/index.js';
import '@carbon/web-components/es/components/icon-button/index.js';
import { alertView } from '../alert/view.ts';
import { deleteObject } from '../filer/httpclient.ts';
import { objectFileName, type FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const ObjectDeleteTag = 'gf-object-delete';

// How many of the files are listed, of more, which are counted
const MaxNamed = 100;

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class ObjectDeleteElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        objects: { state: true },
        error: { state: true },
    };

    // client is the server's, of which the files are deleted
    declare client?: Client;

    // objects are the files which are deleted, and error why some couldn't
    // be, which leaves the panel open
    declare private objects: FilerObject[];
    declare private error?: string;

    private readonly panel = new PanelController<string>({ closable: true });
    private resolve?: (deleted: boolean) => void;
    private deleted = false;

    constructor() {
        super();
        this.objects = [];
        this.panel.onClose(() => this.settle());
        this.panel.subscribe(() => this.requestUpdate());
    }

    protected override createRenderRoot() {
        return this;
    }

    // confirm opens the panel of the files, and resolves true once they're
    // deleted, or false once it's closed without them being
    confirm(objects: readonly FilerObject[]): Promise<boolean> {
        this.settle();
        this.objects = [...objects];
        this.error = undefined;
        this.deleted = false;
        this.setHeader();
        this.panel.setActions([this.deleteAction()]);
        this.panel.open(objects.map((object) => object.path).join('\n'));
        return new Promise((resolve) => { this.resolve = resolve; });
    }

    override render() {
        return html`<gs-panel .controller=${this.panel}>${this.objects.length ? this.listView() : nothing}</gs-panel>`;
    }

    // listView is what's deleted: the files, the first of them, of more,
    // and why some couldn't be, when they couldn't
    private listView(): TemplateResult {
        const named = this.objects.slice(0, MaxNamed);
        const more = this.objects.length - named.length;
        return html`
            <div class="gf-volume-form">
                <p>${this.objects.length === 1 ? 'This file is' : 'These files are'} deleted from the volume, and its backend, such as its folder, or bucket, which can't be undone.</p>
                <cds-contained-list label=${noun(this.objects.length)} kind="on-page">
                    ${named.map((object) => html`
                        <cds-contained-list-item title=${object.path}>
                            ${iconLoader(Document16, { slot: 'icon' })}
                            ${objectFileName(object)}
                            <cds-icon-button slot="action" kind="ghost" size="sm" align="left" ?disabled=${this.panel.running}
                                @click=${() => this.keep(object)}>
                                ${iconLoader(Close16, { slot: 'icon' })}
                                <span slot="tooltip-content">Keep it</span>
                            </cds-icon-button>
                        </cds-contained-list-item>
                    `)}
                    ${more > 0 ? html`<cds-contained-list-item>and ${more.toLocaleString()} more</cds-contained-list-item>` : nothing}
                </cds-contained-list>
                ${this.error ? alertView('error', "Some files couldn't be deleted", this.error) : nothing}
            </div>
        `;
    }

    // keep keeps a file, rather than deleting it, and closes the panel once
    // none are deleted
    private keep(object: FilerObject) {
        this.objects = this.objects.filter((o) => o !== object);
        if (this.objects.length === 0) {
            this.panel.close();
            return;
        }
        this.setHeader();
    }

    // setHeader says how many files are deleted
    private setHeader() {
        this.panel.setHeader({ title: `Delete ${noun(this.objects.length)}`, subtitle: this.objects[0]?.name ?? '' });
    }

    // deleteAction is Delete, which deletes the files, one at a time, and
    // closes the panel once they're all deleted, or else says which
    // couldn't be, and leaves those to be deleted again
    private deleteAction(): PanelAction {
        return {
            label: 'Delete', kind: 'danger', icon: TrashCan16, close: false,
            run: async () => {
                const client = this.client;
                if (!client) {
                    return;
                }
                const failed: FilerObject[] = [];
                let reason = '';

                // It can't be closed while they're deleted, so whether they
                // were is known once it's closed
                this.panel.setClosable(false);
                try {
                    for (const object of this.objects) {
                        try {
                            await deleteObject(client, object.name, object.path);
                            this.deleted = true;
                        } catch (error) {
                            failed.push(object);
                            reason ||= error instanceof Error ? error.message : String(error);
                        }
                    }
                } finally {
                    this.panel.setClosable(true);
                }
                if (failed.length === 0) {
                    this.panel.close();
                    return;
                }
                this.objects = failed;
                this.setHeader();
                this.error = `${failed.length.toLocaleString()} of them: ${reason}`;
            },
        };
    }

    // settle resolves what's confirmed, of whether any files were deleted
    private settle() {
        this.resolve?.(this.deleted);
        this.resolve = undefined;
    }
}

// noun returns how many files there are, such as 2 files
function noun(count: number): string {
    return `${count.toLocaleString()} ${count === 1 ? 'file' : 'files'}`;
}

if (!customElements.get(ObjectDeleteTag)) {
    customElements.define(ObjectDeleteTag, ObjectDeleteElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectDeleteTag]: ObjectDeleteElement;
    }
}
