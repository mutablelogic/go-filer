// The panel which adds a volume, of a backend: its name, its backend's
// fields, such as a directory's path, or a bucket, whether it's writable,
// and how it's searched. Its description is the backend's, such as a
// feed's title, which can be edited once it's added.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import '@mutablelogic/go-server/panel/view';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/content-switcher/index.js';
import '@carbon/web-components/es/components/button/index.js';
import { authorizeSignIn, createVolume } from '../filer/httpclient.ts';
import type { Backend } from '../filer/schema.ts';
import type { VolumesController } from './controller.ts';
import { canBeWritable, signInMessage, signInNeeded, volumeCreate, VolumeFields, type SignInNeeded, type VolumeDraft } from './create.ts';
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
        signIn: { state: true },
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

    // signIn is how the volume's user signs in, when they need to, before
    // it's added, such as with Google, which they do in a window which it
    // opens, of the server's callback, which tells this one, once they have
    declare private signIn?: SignInNeeded;

    private readonly panel = new PanelController<string>({ closable: true });
    // addAction is Add, which closes the panel itself, once the volume is
    // added, so it stays open when its user needs to sign in first
    private readonly addAction: PanelAction = { label: 'Add', kind: 'primary', close: false, run: () => this.add() };
    private readonly onMessage = (event: MessageEvent) => this.signedIn(event);

    constructor() {
        super();
        this.busy = false;
        this.panel.onError((error) => this.notifications?.error("The volume couldn't be added", error));
    }

    override connectedCallback() {
        super.connectedCallback();
        window.addEventListener('message', this.onMessage);
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        window.removeEventListener('message', this.onMessage);
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel which adds a volume of the backend
    open(backend: Backend) {
        this.backend = backend;
        this.signIn = undefined;
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

    // actions are Add, as the panel is closed of its close button, the
    // escape key, or the page beside it
    private actions(): PanelAction[] {
        return [this.addAction];
    }

    // formView is the form of the volume: its name, the backend's fields, its access, unless it can't be writable, and how
    // it's searched, each of what the one which is chosen does
    private formView(draft: VolumeDraft): TemplateResult {
        const fields = VolumeFields.get(draft.scheme) ?? [];
        return html`
            <div class="gf-volume-form" ?inert=${this.busy}>
                ${this.signIn ? html`
                    <div class="gf-volume-signin">
                        <p>The volume needs a sign-in with Google. Sign in, in the window which opens, and it's added.</p>
                        <cds-button kind="tertiary" size="md" @click=${() => void this.openSignIn()}>Sign in with Google</cds-button>
                    </div>
                ` : nothing}
                <cds-text-input label="Name" .value=${draft.name} placeholder="media" autocomplete="off"
                    helper-text="An identifier, of letters, digits, - and _, which is the volume's, in its links"
                    @input=${(event: Event) => this.set({ name: (event.target as HTMLInputElement).value })}></cds-text-input>
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
    // the panel can't be closed, nor what's typed be changed, so an add
    // which ends doesn't close the panel of another, which is opened
    // meanwhile. When its user
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
            this.signIn = undefined;
            await this.controller.reload();
        } catch (error) {
            // When its user needs to sign in, they're asked to, in the panel,
            // which stays open, rather than told it failed
            const signIn = signInNeeded(error);
            if (signIn) {
                this.signIn = signIn;
                return;
            }
            throw error;
        } finally {
            this.busy = false;
            this.panel.setClosable(true);
        }
        this.panel.close();
    }

    // openSignIn opens a window, at once, as it's of a click, which a
    // browser allows, at the URL to sign in at, which the server makes. When
    // the server has no web client, the window is closed, and it says to
    // sign in with the CLI.
    private async openSignIn() {
        const signIn = this.signIn;
        const draft = this.draft;
        if (!signIn || !draft || !this.controller) {
            return;
        }
        const popup = window.open('', 'gf-signin', 'popup,width=520,height=680');
        try {
            const url = await authorizeSignIn(this.controller.client, signIn.key, signIn.scopes);
            if (popup) {
                popup.location.href = url;
            } else {
                window.location.href = url;
            }
        } catch (error) {
            popup?.close();
            const name = draft.name.trim() || '<name>';
            this.notifications?.error("You can't sign in here", new Error(`${error instanceof Error ? error.message : String(error)}. The server needs a web client, ${'GOOGLE_WEB_CLIENT'}, to sign in in a browser, so sign in, and add the volume, with the CLI: filer volume-create-${draft.scheme} ${name}`));
        }
    }

    // signedIn adds the volume again, once its user has signed in, of the
    // message the server's callback posts, or says why they aren't
    private signedIn(event: MessageEvent) {
        const message = signInMessage(event, location.origin);
        if (!message || !this.signIn || message.key !== this.signIn.key) {
            return;
        }
        if (!message.ok) {
            this.notifications?.error("You aren't signed in", new Error(message.error || 'The sign-in was refused'));
            return;
        }
        this.signIn = undefined;
        void this.panel.run(this.addAction);
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
