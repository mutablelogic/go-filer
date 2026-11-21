// The panel of a volume, of four tabs: View, its details, of which it's
// mounted, or unmounted, Edit, its description, access and index, and
// Options, its other options, both of which are saved together, and Remove,
// which removes it from the filer, once its name is typed. When its user
// needs to sign in to mount it, such as with Google, the View tab asks them
// to, and mounts it, once they have

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import { PanelController, type PanelAction, type PanelTab } from '@mutablelogic/go-server/panel';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import '@mutablelogic/go-server/panel/view';
import '@carbon/web-components/es/components/text-input/index.js';
import { deleteVolume, updateVolume } from '../filer/httpclient.ts';
import { signInMessage, signInNeeded, type SignInNeeded } from './create.ts';
import { openSignIn, signInPrompt } from './signin.ts';
import { volumeBackend, volumeLabel, volumeNoun, volumeObjects, volumeStatus, type Volume } from '../filer/schema.ts';
import type { VolumesController } from './controller.ts';
import { IndexModes, volumeOptions } from './options.ts';
import './edit.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// VolumeTab is a tab of a volume's panel
export type VolumeTab = 'view' | 'edit' | 'options' | 'remove';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const VolumePanelTag = 'gf-volume-panel';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// VolumePanelElement is the panel of a volume, which open opens, of one of
// its tabs, and which follows the volume, as it changes, of the controller
export class VolumePanelElement extends LitElement {
    static override properties = {
        controller: { attribute: false },
        notifications: { attribute: false },
        name: { state: true },
        tab: { state: true },
        confirm: { state: true },
        volume: { state: true },
        busy: { state: true },
        signIn: { state: true },
    };

    // controller keeps the volumes, and notifications are told why an action
    // failed
    declare controller?: VolumesController;
    declare notifications?: NotificationController;

    // name is the volume's, while the panel is open, tab is the tab which is
    // chosen, confirm is the name which is typed, to remove it, and volume is
    // the volume, as it is now
    declare private name?: string;
    declare private tab?: VolumeTab;
    declare private confirm: string;
    declare private volume?: Volume;

    // busy is whether the volume is saved, or removed, while which its form,
    // or the name which is typed, can't be changed, as what's saved, or
    // removed, is of them as they were
    declare private busy: boolean;

    // signIn is how the volume's user signs in, when they need to, to mount
    // it, which they do in a window which it opens, of the server's
    // callback, which tells this one, once they have
    declare private signIn?: SignInNeeded;

    private readonly panel = new PanelController<string>({ closable: true });
    private unsubscribe?: () => void;

    // The actions' functions, which are the same, so an action is the same
    // action when it's set again, such as once it's enabled
    private readonly mountRun = () => this.setEnabled(true);
    private readonly unmountRun = () => this.setEnabled(false);
    private readonly saveRun = () => this.save();
    private readonly removeRun = () => this.removeVolume();
    private readonly onMessage = (event: MessageEvent) => this.signedIn(event);

    constructor() {
        super();
        this.confirm = '';
        this.busy = false;
        this.panel.subscribe((state) => {
            const tab = state.tab as VolumeTab | undefined;
            const changed = tab !== this.tab;
            this.name = state.open ? state.item : undefined;
            this.tab = tab;
            if (changed && tab === 'view') {
                this.refresh();
            }
        });
        this.panel.onError((error, action) => this.notifications?.error(`The volume couldn't be ${failed(action)}`, error));
    }

    protected override createRenderRoot() {
        return this;
    }

    override connectedCallback() {
        super.connectedCallback();
        window.addEventListener('message', this.onMessage);
        this.unsubscribe?.();
        this.unsubscribe = this.controller?.subscribe(() => this.refresh());
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        window.removeEventListener('message', this.onMessage);
        this.unsubscribe?.();
        this.unsubscribe = undefined;
    }

    // open opens the panel of the volume, of the tab, which is View, by
    // default
    open(volume: Volume, tab: VolumeTab = 'view') {
        if (!this.unsubscribe && this.controller) {
            this.unsubscribe = this.controller.subscribe(() => this.refresh());
        }
        const backend = volumeBackend(volume, this.controller?.backends);
        this.confirm = '';
        this.signIn = undefined;
        this.volume = volume;
        this.panel.setHeader({ title: volumeLabel(volume), subtitle: backend ? `${volume.name} · ${backend}` : volume.name });
        this.panel.setTabs(this.tabs(volume), tab);
        this.panel.open(volume.name);
    }

    override render() {
        const volume = this.volume;
        return html`
            <gs-panel .controller=${this.panel}>
                ${volume && this.tab === 'view' ? this.viewView(volume) : nothing}
                <gf-volume-form ?hidden=${this.tab !== 'edit' && this.tab !== 'options'} ?inert=${this.busy}
                    section=${this.tab === 'options' ? 'options' : 'edit'} .volume=${this.name ? volume : undefined}></gf-volume-form>
                ${volume && this.tab === 'remove' ? this.removeView(volume) : nothing}
            </gs-panel>
        `;
    }

    // tabs are the panel's tabs, of their actions
    private tabs(volume: Volume): PanelTab[] {
        return [
            { value: 'view', label: 'View', actions: this.mountActions(volume) },
            { value: 'edit', label: 'Edit', actions: [{ label: 'Save', kind: 'primary', run: this.saveRun }] },
            {
                value: 'options', label: 'Options', disabled: volumeOptions(volume).others.length === 0,
                actions: [{ label: 'Save', kind: 'primary', run: this.saveRun }],
            },
            { value: 'remove', label: 'Remove', actions: [this.removeAction(volume)] },
        ];
    }

    // mountActions are Unmount, unless it's unmounted, and Mount, unless
    // it's mounted, which leave the panel open, so it's seen to change
    private mountActions(volume: Volume): PanelAction[] {
        const enabled = volume.enabled || Boolean(volume.error);
        return [
            { label: 'Unmount', kind: 'secondary', close: false, disabled: !enabled, run: this.unmountRun },
            { label: 'Mount', kind: 'primary', close: false, disabled: volume.mounted && !volume.error, run: this.mountRun },
        ];
    }

    // removeAction is Remove, which is enabled once the volume's name is
    // typed
    private removeAction(volume: Volume): PanelAction {
        return { label: 'Remove', kind: 'danger', disabled: this.confirm !== volume.name, run: this.removeRun };
    }

    // refresh follows the volume which is open, as it changes, of its View
    // tab's actions, while it's chosen
    private refresh() {
        const volume = this.name ? this.controller?.volume(this.name) : undefined;
        if (!volume) {
            return;
        }
        this.volume = volume;
        if (this.tab === 'view') {
            this.panel.setActions(this.mountActions(volume));
        }
    }

    // viewView is the volume's details, and what unmounting it does, of its
    // actions
    private viewView(volume: Volume): TemplateResult {
        const options = volumeOptions(volume);
        const objects = volumeObjects(volume);
        const mode = IndexModes.find((m) => m.value === options.index)?.label ?? options.index;
        const error = volume.error || volume.index?.error || volume.index?.metadata?.error;
        const rows: [string, string][] = [
            ['Description', options.description || '–'],
            ['Name', volume.name],
            ['Backend', volumeBackend(volume, this.controller?.backends) || '–'],
            ['Status', volumeStatus(volume)],
            ['Access', options.readonly === undefined ? 'Read only' : options.readonly ? 'Read only' : 'Writable'],
            ['Search', mode],
            ['Folders', volume.index?.folders?.toLocaleString() ?? '–'],
            [volumeNoun(volume, 2).replace(/^./, (c) => c.toUpperCase()), objects?.toLocaleString() ?? '–'],
            ['URL', volume.url],
        ];
        return html`
            ${this.signIn ? signInPrompt("it's mounted", () => void this.openSignIn()) : nothing}
            <dl class="gf-volume-details">
                ${rows.map(([key, value]) => html`<dt>${key}</dt><dd>${value}</dd>`)}
                ${error ? html`<dt>Error</dt><dd class="gf-volume-tile-error">${error}</dd>` : nothing}
            </dl>
            <p class="gf-volume-view-note">A volume can be unmounted to exclude it from search and browsing.</p>
        `;
    }

    // removeView asks for the volume's name, before it's removed
    private removeView(volume: Volume): TemplateResult {
        return html`
            <div class="gf-volume-form gf-volume-remove" ?inert=${this.busy}>
                <p>This removes the volume from the filer, which also removes its index and cache.</p>
                <cds-text-input label=${`Type "${volume.name}" to remove it`} .value=${this.confirm} autocomplete="off"
                    @input=${(event: Event) => this.setConfirm(volume, (event.target as HTMLInputElement).value)}></cds-text-input>
            </div>
        `;
    }

    // setConfirm sets the name which is typed, which enables Remove, once it's
    // the volume's
    private setConfirm(volume: Volume, value: string) {
        this.confirm = value;
        this.panel.setActions([this.removeAction(volume)]);
    }

    // setBusy sets whether the volume is saved, or removed, while which the
    // panel can't be closed, and its form, or the name which is typed, can't
    // be changed, and are again, once it's done, or it fails
    private setBusy(busy: boolean) {
        this.busy = busy;
        this.panel.setClosable(!busy);
    }

    // setEnabled mounts the volume, or unmounts it, and loads the volumes
    // again. When its user needs to sign in to mount it, they're asked to,
    // rather than told it failed.
    private async setEnabled(enabled: boolean) {
        const name = this.name;
        if (!name || !this.controller) {
            return;
        }
        try {
            await updateVolume(this.controller.client, name, {}, enabled);
            this.signIn = undefined;
        } catch (error) {
            const signIn = enabled ? signInNeeded(error) : undefined;
            if (!signIn) {
                throw error;
            }
            this.signIn = signIn;
        }
        await this.controller.reload();
    }

    // openSignIn opens a window, which signs in, or says why it can't, and
    // to mount the volume with the CLI instead
    private async openSignIn() {
        const signIn = this.signIn;
        if (!signIn || !this.name || !this.controller) {
            return;
        }
        try {
            await openSignIn(this.controller.client, signIn, `filer volume-mount ${this.name}`);
        } catch (error) {
            this.notifications?.error("You can't sign in here", error);
        }
    }

    // signedIn mounts the volume again, once its user has signed in, of the
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
        const mount = this.volume && this.mountActions(this.volume).find((action) => action.run === this.mountRun);
        if (mount) {
            void this.panel.run(mount);
        }
    }

    // save saves the options of the volume which are changed, of its form,
    // and loads the volumes again, rather than only once they change, as an
    // app needn't follow their changes. The panel can't be closed, nor the
    // form changed, meanwhile, so a save which ends doesn't close the panel
    // of another volume, nor are changes made after it's saved lost.
    private async save() {
        const name = this.name;
        const patch = this.querySelector('gf-volume-form')?.patch() ?? {};
        if (!name || Object.keys(patch).length === 0 || !this.controller) {
            return;
        }
        this.setBusy(true);
        try {
            await updateVolume(this.controller.client, name, patch);
            await this.controller.reload();
        } finally {
            this.setBusy(false);
        }
    }

    // removeVolume removes the volume, and its index and cache, but not what's
    // its backend's, once its name is typed, and loads the volumes
    // again
    private async removeVolume() {
        const name = this.name;
        if (!name || this.confirm !== name || !this.controller) {
            return;
        }
        this.setBusy(true);
        try {
            await deleteVolume(this.controller.client, name);
            await this.controller.reload();
        } finally {
            this.setBusy(false);
        }
    }
}

// failed returns what an action failed to do, of its label
function failed(action: PanelAction): string {
    switch (action.label) {
        case 'Mount':
            return 'mounted';
        case 'Unmount':
            return 'unmounted';
        case 'Remove':
            return 'removed';
        default:
            return 'saved';
    }
}

if (!customElements.get(VolumePanelTag)) {
    customElements.define(VolumePanelTag, VolumePanelElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [VolumePanelTag]: VolumePanelElement;
    }
}
