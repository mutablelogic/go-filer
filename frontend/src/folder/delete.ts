// The panel which deletes files of a writable volume, once it's confirmed:
// which files, and that they're deleted from the volume's backend, such as
// a folder, or a bucket, which can't be undone. confirm opens it, and
// resolves once they're deleted, or it's closed without them being.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import TrashCan16 from '@carbon/icons/es/trash-can/16.js';
import { alertView } from '../alert/view.ts';
import { deleteObject } from '../filer/httpclient.ts';
import { objectFileName, type FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const ObjectDeleteTag = 'gf-object-delete';

// How many of the files are named, of more, which are counted
const MaxNamed = 10;

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
        const noun = objects.length === 1 ? 'file' : 'files';
        this.panel.setHeader({ title: `Delete ${objects.length.toLocaleString()} ${noun}`, subtitle: objects[0]?.name ?? '' });
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
                <ul class="gf-object-delete-list">
                    ${named.map((object) => html`<li title=${object.path}>${objectFileName(object)}</li>`)}
                    ${more > 0 ? html`<li>and ${more.toLocaleString()} more</li>` : nothing}
                </ul>
                ${this.error ? alertView('error', "Some files couldn't be deleted", this.error) : nothing}
            </div>
        `;
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

if (!customElements.get(ObjectDeleteTag)) {
    customElements.define(ObjectDeleteTag, ObjectDeleteElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectDeleteTag]: ObjectDeleteElement;
    }
}
