// The panel which adds a volume, of a backend: its name, its description,
// its backend's fields, such as a directory's path, or a bucket, whether
// it's writable, and how it's searched

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import '@mutablelogic/go-server/panel/view';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/content-switcher/index.js';
import { createVolume } from '../filer/httpclient.ts';
import type { Backend } from '../filer/schema.ts';
import type { VolumesController } from './controller.ts';
import { canBeWritable, signInNeeded, volumeCreate, VolumeFields, type VolumeDraft } from './create.ts';
import { AccessDescriptions, IndexModes, indexDescription } from './options.ts';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const VolumeAddTag = 'gf-volume-add';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// VolumeAddElement is the panel which adds a volume, of a backend, which
// open opens, which is read-only, and searched, by default
export class VolumeAddElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        notifications: { attribute: false },
        backend: { state: true },
        draft: { state: true },
        busy: { state: true },
    };

    // controller keeps the volumes, and notifications are told why one
    // can't be added
    declare controller?: VolumesController;
    declare notifications?: NotificationController;

    // backend is the backend of the volume which is added, draft is what's
    // typed, and busy is whether it's added, while which it can't be changed
    declare private backend?: Backend;
    declare private draft?: VolumeDraft;
    declare private busy: boolean;

    private readonly panel = new PanelController<string>({ closable: true });
    private readonly addRun = () => this.add();

    constructor() {
        super();
        this.busy = false;
        this.panel.onError((error) => this.notifications?.error("The volume couldn't be added", error));
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel which adds a volume of the backend
    open(backend: Backend) {
        this.backend = backend;
        this.draft = { scheme: backend.scheme, name: '', description: '', readonly: true, index: 'objects', fields: {} };
        this.panel.setHeader({ title: `Add ${backend.description || backend.scheme}`, subtitle: `A volume of ${backend.scheme}` });
        this.panel.setActions(this.actions());
        this.panel.open(backend.scheme);
    }

    override render() {
        const draft = this.draft;
        return html`
            <gs-panel .controller=${this.panel}>
                ${draft ? this.formView(draft) : nothing}
            </gs-panel>
        `;
    }

    // actions are Cancel, and Add
    private actions(): PanelAction[] {
        return [
            { label: 'Cancel', kind: 'secondary' },
            { label: 'Add', kind: 'primary', run: this.addRun },
        ];
    }

    // formView is the form of the volume: its name, description, the
    // backend's fields, its access, unless it can't be writable, and how
    // it's searched, each of what the one which is chosen does
    private formView(draft: VolumeDraft): TemplateResult {
        const fields = VolumeFields.get(draft.scheme) ?? [];
        return html`
            <div class="gf-volume-form" ?inert=${this.busy}>
                <cds-text-input label="Name" .value=${draft.name} placeholder="media" autocomplete="off"
                    helper-text="An identifier, of letters, digits, - and _, which is the volume's, in its links"
                    @input=${(event: Event) => this.set({ name: (event.target as HTMLInputElement).value })}></cds-text-input>
                <cds-text-input label="Description" .value=${draft.description}
                    helper-text="What the volume is, which is its label, or else the backend's"
                    @input=${(event: Event) => this.set({ description: (event.target as HTMLInputElement).value })}></cds-text-input>
                ${fields.map((field) => html`
                    <cds-text-input label=${field.required ? field.label : `${field.label} (optional)`} .value=${draft.fields[field.key] ?? ''}
                        placeholder=${field.placeholder ?? ''} helper-text=${field.helper ?? ''} autocomplete="off"
                        @input=${(event: Event) => this.setField(field.key, (event.target as HTMLInputElement).value)}></cds-text-input>
                `)}
                ${canBeWritable(draft.scheme) ? html`
                    <div class="gf-volume-form-field">
                        <p class="cds--label">Access</p>
                        <cds-content-switcher size="sm" value=${draft.readonly ? 'readonly' : 'writable'}
                            @cds-content-switcher-selected=${(event: CustomEvent<{ item: HTMLElement & { value: string } }>) => this.set({ readonly: event.detail.item.value === 'readonly' })}>
                            <cds-content-switcher-item value="readonly">Read only</cds-content-switcher-item>
                            <cds-content-switcher-item value="writable">Writable</cds-content-switcher-item>
                        </cds-content-switcher>
                        <p class="gf-volume-form-helper">${draft.readonly ? AccessDescriptions.readonly : AccessDescriptions.writable}</p>
                    </div>
                ` : nothing}
                <div class="gf-volume-form-field">
                    <p class="cds--label">Search</p>
                    <cds-content-switcher size="sm" value=${draft.index}
                        @cds-content-switcher-selected=${(event: CustomEvent<{ item: HTMLElement & { value: string } }>) => this.set({ index: event.detail.item.value })}>
                        ${IndexModes.map((mode) => html`<cds-content-switcher-item value=${mode.value} ?disabled=${mode.disabled}>${mode.label}</cds-content-switcher-item>`)}
                    </cds-content-switcher>
                    <p class="gf-volume-form-helper">${indexDescription(draft.index)}</p>
                </div>
            </div>
        `;
    }

    // set changes what's typed
    private set(changes: Partial<VolumeDraft>) {
        if (this.draft) {
            this.draft = { ...this.draft, ...changes };
        }
    }

    // setField changes a field of the backend's which is typed
    private setField(key: string, value: string) {
        if (this.draft) {
            this.draft = { ...this.draft, fields: { ...this.draft.fields, [key]: value } };
        }
    }

    // add creates the volume, of its URL, and loads the volumes again, while
    // the panel can't be closed, nor what's typed be changed. When its user
    // needs to sign in, such as with Google, which can't be done here yet,
    // it says to with the CLI.
    private async add() {
        const draft = this.draft;
        if (!draft || !this.controller) {
            return;
        }
        const volume = volumeCreate(draft);
        this.busy = true;
        this.panel.setClosable(false);
        try {
            await createVolume(this.controller.client, volume.name, volume.url);
        } catch (error) {
            if (signInNeeded(error)) {
                throw new Error(`It needs a sign-in with Google, which can't be done here yet. Sign in, and add it, with the CLI: filer volume-create-${draft.scheme} ${volume.name}`);
            }
            throw error;
        } finally {
            this.busy = false;
            this.panel.setClosable(true);
        }
        await this.controller.reload();
    }
}

if (!customElements.get(VolumeAddTag)) {
    customElements.define(VolumeAddTag, VolumeAddElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [VolumeAddTag]: VolumeAddElement;
    }
}
