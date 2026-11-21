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
import { createVolume } from '../filer/httpclient.ts';
import type { Backend } from '../filer/schema.ts';
import type { VolumesController } from './controller.ts';
import { canBeWritable, signInMessage, signInNeeded, volumeCreate, VolumeFields, type SignInNeeded, type VolumeDraft } from './create.ts';
import { AccessDescriptions, IndexModes, indexDescription } from './options.ts';
import { openSignIn, SignInLabel, signInPrompt } from './signin.ts';

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
    // signInAction is Sign in, in place of Add, when its user needs to sign
    // in first, which opens a window, at once, as it's of a click
    private readonly signInAction: PanelAction = { label: SignInLabel, kind: 'primary', close: false, run: () => this.openSignIn() };
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

    // actions are Add, or Sign in, when its user needs to sign in first, as
    // the panel is closed of its close button, the escape key, or the page
    // beside it
    private actions(): PanelAction[] {
        return [this.signIn ? this.signInAction : this.addAction];
    }

    // setSignIn sets how the volume's user signs in, when they need to, of
    // the panel's action
    private setSignIn(signIn?: SignInNeeded) {
        this.signIn = signIn;
        this.panel.setActions(this.actions());
    }

    // formView is the form of the volume: its name, the backend's fields, its access, unless it can't be writable, and how
    // it's searched, each of what the one which is chosen does
    private formView(draft: VolumeDraft): TemplateResult {
        const fields = VolumeFields.get(draft.scheme) ?? [];
        return html`
            <div class="gf-volume-form" ?inert=${this.busy}>
                ${this.signIn ? signInPrompt("it's added") : nothing}
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
            await this.controller.reload();
        } catch (error) {
            // When its user needs to sign in, they're asked to, in the panel,
            // which stays open, rather than told it failed
            const signIn = signInNeeded(error);
            if (signIn) {
                this.setSignIn(signIn);
                return;
            }
            throw error;
        } finally {
            this.busy = false;
            this.panel.setClosable(true);
        }
        this.panel.close();
    }

    // openSignIn opens a window, which signs in, or says why it can't, and
    // to add the volume with the CLI instead
    private async openSignIn() {
        const signIn = this.signIn;
        const draft = this.draft;
        if (!signIn || !draft || !this.controller) {
            return;
        }
        try {
            await openSignIn(this.controller.client, signIn, `filer volume-create-${draft.scheme} ${draft.name.trim() || '<name>'}`);
        } catch (error) {
            this.notifications?.error("You can't sign in here", error);
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
        this.setSignIn(undefined);
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
