// The form of a volume's options, which a panel shows when it's edited: its
// description, whether it's writable, and how it's indexed, of one section,
// and its other options, of another, of whose changes a patch is made

import { LitElement, html, nothing, type PropertyValues, type TemplateResult } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import type { Volume } from '../filer/schema.ts';
import { AccessDescriptions, IndexModes, indexDescription, isMany, volumeOptions, volumePatch, type VolumeOptions, type VolumePatch } from './options.ts';
import '@carbon/web-components/es/components/text-input/index.js';
import '@carbon/web-components/es/components/textarea/index.js';
import '@carbon/web-components/es/components/content-switcher/index.js';

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const VolumeFormTag = 'gf-volume-form';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// VolumeFormElement is the form of a volume's options, which are those of its
// URL, as they were when it was given the volume, which it keeps, as they're
// edited, while the volume changes, such as while it's indexed, until it's
// given another
export class VolumeFormElement extends LitElement {
    static override properties = {
        volume: { attribute: false },
        section: { type: String },
        edited: { state: true },
    };

    // volume is the volume which is edited, and section is the part of its
    // form which is shown: edit, its description, access and index, or
    // options, its others, both of which are kept, while the other is shown
    declare volume?: Volume;
    declare section: 'edit' | 'options';
    declare private edited?: VolumeOptions;
    private original?: VolumeOptions;
    private name?: string;

    constructor() {
        super();
        this.section = 'edit';
    }

    protected override createRenderRoot() {
        return this;
    }

    protected override willUpdate(changed: PropertyValues<this>) {
        if (changed.has('volume') && this.volume?.name !== this.name) {
            this.name = this.volume?.name;
            this.original = this.volume ? volumeOptions(this.volume) : undefined;
            this.edited = this.original ? structuredClone(this.original) : undefined;
        }
    }

    // patch returns the options which are changed, which is empty when none
    // are
    patch(): VolumePatch {
        return this.original && this.edited ? volumePatch(this.original, this.edited) : {};
    }

    override render() {
        const edited = this.edited;
        if (!edited) {
            return nothing;
        }
        return html`
            <div class="gf-volume-form" ?hidden=${this.section !== 'edit'}>
                <cds-text-input label="Description" .value=${edited.description}
                    helper-text="What the volume is, which is its label"
                    @input=${(event: Event) => this.set({ description: (event.target as HTMLInputElement).value })}></cds-text-input>
                ${edited.readonly !== undefined ? html`
                    <div class="gf-volume-form-field">
                        <p class="cds--label">Access</p>
                        <cds-content-switcher size="sm" value=${edited.readonly ? 'readonly' : 'writable'}
                            @cds-content-switcher-selected=${(event: CustomEvent<{ item: HTMLElement & { value: string } }>) => this.set({ readonly: event.detail.item.value === 'readonly' })}>
                            <cds-content-switcher-item value="readonly">Read only</cds-content-switcher-item>
                            <cds-content-switcher-item value="writable">Writable</cds-content-switcher-item>
                        </cds-content-switcher>
                        <p class="gf-volume-form-helper">${edited.readonly ? AccessDescriptions.readonly : AccessDescriptions.writable}</p>
                    </div>
                ` : nothing}
                <div class="gf-volume-form-field">
                    <p class="cds--label">Search</p>
                    <cds-content-switcher size="sm" value=${edited.index}
                        @cds-content-switcher-selected=${(event: CustomEvent<{ item: HTMLElement & { value: string } }>) => this.set({ index: event.detail.item.value })}>
                        ${IndexModes.map((mode) => html`<cds-content-switcher-item value=${mode.value} ?disabled=${mode.disabled}>${mode.label}</cds-content-switcher-item>`)}
                    </cds-content-switcher>
                    <p class="gf-volume-form-helper">${indexDescription(edited.index)}</p>
                </div>
            </div>
            <div class="gf-volume-form" ?hidden=${this.section !== 'options'}>
                ${edited.others.length
                    ? repeat(edited.others, ([key]) => key, ([key, values], i) => this.optionView(key, values, i))
                    : html`<p>The volume has no other options.</p>`}
            </div>
        `;
    }

    // optionView is an option's field: a text input, of one value, or a text
    // area of many, of a line each
    private optionView(key: string, values: readonly string[], i: number): TemplateResult {
        const original = this.original?.others.find(([k]) => k === key)?.[1] ?? [];
        if (isMany(key, original)) {
            return html`
                <cds-textarea label=${key} rows=${Math.min(Math.max(values.length, 2), 8)} .value=${values.join('\n')}
                    helper-text="One a line"
                    @input=${(event: Event) => this.setOption(i, (event.target as HTMLTextAreaElement).value.split('\n'))}></cds-textarea>
            `;
        }
        return html`
            <cds-text-input label=${key} .value=${values[0] ?? ''}
                @input=${(event: Event) => this.setOption(i, [(event.target as HTMLInputElement).value])}></cds-text-input>
        `;
    }

    // set changes the options which are edited
    private set(options: Partial<VolumeOptions>) {
        if (this.edited) {
            this.edited = { ...this.edited, ...options };
        }
    }

    // setOption changes the values of an option which is edited
    private setOption(i: number, values: string[]) {
        if (this.edited) {
            const others = [...this.edited.others];
            others[i] = [others[i][0], values];
            this.edited = { ...this.edited, others };
        }
    }
}

if (!customElements.get(VolumeFormTag)) {
    customElements.define(VolumeFormTag, VolumeFormElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [VolumeFormTag]: VolumeFormElement;
    }
}
