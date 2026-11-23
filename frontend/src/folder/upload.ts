// The panel which adds files to a folder of a writable volume: files which
// are dropped on it, or chosen, and folders, of every file in them, of
// their paths, so a folder is added in the folder, with its folders, which
// are uploaded into the folder, of one request, replacing those which exist, when it overwrites them, and
// otherwise skipping them, and then which were added, and which skipped,
// and why. open opens it, of the folder, and the function which is called
// once files are added.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import Upload16 from '@carbon/icons/es/upload/16.js';
import '@carbon/web-components/es/components/file-uploader/index.js';
import '@carbon/web-components/es/components/toggle/index.js';
import '@carbon/web-components/es/components/button/index.js';
import { alertView } from '../alert/view.ts';
import { uploadObjects } from '../filer/httpclient.ts';
import type { ObjectSkipped } from '../filer/schema.ts';
import { formatSize } from './format.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Chosen is a file which is chosen, of its path in the folder it's added
// to, such as its name, or of a folder's, such as Photos/a.jpg, and, once
// it's uploaded, whether it was, or was skipped, and why
interface Chosen {
    file: File;
    path: string;
    state: 'edit' | 'uploading' | 'complete';
    skipped?: ObjectSkipped;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const ObjectUploadTag = 'gf-object-upload';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class ObjectUploadElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        chosen: { state: true },
        overwrite: { state: true },
        error: { state: true },
    };

    // client is the server's, to which the files are uploaded
    declare client?: Client;

    // chosen are the files which are chosen, overwrite whether those which
    // exist are replaced, and error why they couldn't be uploaded
    declare private chosen: Chosen[];
    declare private overwrite: boolean;
    declare private error?: string;

    private volume = '';
    private folder = '/';
    private added?: () => void;
    private readonly panel = new PanelController<string>({ closable: true });

    // session is of each time the panel is opened, and closed, so what's
    // read, or uploaded, of one before, isn't of this one
    private session = 0;

    constructor() {
        super();
        this.chosen = [];
        this.overwrite = false;
        this.panel.onClose(() => { this.session++; });
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel of the volume's folder, and calls the function
    // once files are added to it
    open(volume: string, folder: string, added?: () => void) {
        this.session++;
        this.volume = volume;
        this.folder = folder;
        this.added = added;
        this.chosen = [];
        this.overwrite = false;
        this.error = undefined;
        this.panel.setHeader({ title: 'Add files', subtitle: `${volume}${folder === '/' ? '' : folder}` });
        this.setAction();
        this.panel.open(`${volume}${folder}`);
    }

    override render() {
        return html`<gs-panel .controller=${this.panel}>${this.volume ? this.formView() : nothing}</gs-panel>`;
    }

    // formView is where files are dropped, or chosen, which are listed, of
    // their sizes, and whether those which exist are replaced
    private formView(): TemplateResult {
        const uploading = this.chosen.some((c) => c.state === 'uploading');
        const done = this.chosen.length > 0 && this.chosen.every((c) => c.state === 'complete');
        return html`
            <div class="gf-volume-form">
                <cds-file-uploader label-title="Files" label-description="Files, and folders, of all of their files, which are added to the folder">
                    ${done ? nothing : html`
                        <div @drop=${{ handleEvent: (event: DragEvent) => this.dropped(event), capture: true }}>
                            <cds-file-uploader-drop-container multiple ?disabled=${uploading}
                                @cds-file-uploader-drop-container-changed=${(event: CustomEvent<{ addedFiles: File[] }>) => this.add(event.detail.addedFiles.map((file) => ({ file, path: file.name })))}>
                                Drag and drop files, or folders, here, or click to choose files
                            </cds-file-uploader-drop-container>
                        </div>
                        <cds-button kind="ghost" size="sm" ?disabled=${uploading} @click=${() => this.querySelector<HTMLInputElement>('.gf-object-upload-folder')?.click()}>Choose a folder</cds-button>
                        <input class="gf-object-upload-folder" type="file" webkitdirectory multiple hidden
                            @change=${(event: Event) => this.chooseFolder(event.target as HTMLInputElement)}>
                    `}
                    ${this.chosen.map((c, i) => html`
                        <cds-file-uploader-item state=${c.state} ?invalid=${!!c.skipped}
                            error-subject=${c.skipped ? (c.skipped.exists ? 'It exists' : "It wasn't added") : ''}
                            error-body=${c.skipped?.reason ?? ''}
                            @cds-file-uploader-item-deleted=${(event: Event) => { event.preventDefault(); this.unchoose(i); }}>
                            ${c.path} <span class="gf-object-upload-size">${formatSize(c.file.size)}</span>
                        </cds-file-uploader-item>
                    `)}
                </cds-file-uploader>
                ${done ? nothing : html`
                    <cds-toggle size="sm" label-text="Replace files which exist" label-a="Yes" label-b="No" ?checked=${this.overwrite} ?disabled=${uploading}
                        @cds-toggle-changed=${(event: CustomEvent<{ checked: boolean }>) => { this.overwrite = event.detail.checked; }}></cds-toggle>
                `}
                ${this.error ? alertView('error', "The files couldn't be added", this.error) : nothing}
            </div>
        `;
    }

    // add adds files which are dropped, or chosen, of their paths, other
    // than those of the same paths, which are chosen already
    private add(files: readonly { file: File; path: string }[]) {
        const paths = new Set(this.chosen.map((c) => c.path));
        this.chosen = [...this.chosen, ...files.filter((f) => !paths.has(f.path)).map((f) => ({ ...f, state: 'edit' as const }))];
        this.error = undefined;
        this.setAction();
    }

    // dropped adds what's dropped, when it's of folders, of every file in
    // them, of their paths, in place of Carbon's drop container, which only
    // has files, and a folder of none
    private dropped(event: DragEvent) {
        const entries = [...(event.dataTransfer?.items ?? [])].map((item) => item.webkitGetAsEntry()).filter((entry): entry is FileSystemEntry => !!entry);
        if (!entries.some((entry) => entry.isDirectory)) {
            return;
        }
        event.preventDefault();
        event.stopPropagation();
        const session = this.session;
        void Promise.all(entries.map((entry) => entryFiles(entry, ''))).then(
            (files) => {
                if (session === this.session) {
                    this.add(files.flat());
                }
            },
            (error) => {
                if (session === this.session) {
                    this.error = `The folder couldn't be read: ${error instanceof Error ? error.message : String(error)}`;
                }
            },
        );
    }

    // chooseFolder adds the files of a folder which is chosen, of their
    // paths, which start with the folder's name
    private chooseFolder(input: HTMLInputElement) {
        this.add([...(input.files ?? [])].map((file) => ({ file, path: file.webkitRelativePath || file.name })));
        input.value = '';
    }

    // unchoose removes a file which is chosen
    private unchoose(i: number) {
        this.chosen = this.chosen.filter((_, j) => j !== i);
        this.setAction();
    }

    // setAction sets the panel's action: Add, of the files which are chosen,
    // or Done, once they're added
    private setAction() {
        const done = this.chosen.length > 0 && this.chosen.every((c) => c.state === 'complete');
        const action: PanelAction = done
            ? { label: 'Done', kind: 'primary' }
            : { label: 'Add', kind: 'primary', icon: Upload16, close: false, disabled: this.chosen.length === 0, run: () => this.upload() };
        this.panel.setActions([action]);
    }

    // upload uploads the files which are chosen into the folder, and says
    // which were added, and which skipped, and why
    private async upload() {
        const client = this.client;
        const pending = this.chosen.filter((c) => c.state === 'edit');
        if (!client || pending.length === 0) {
            return;
        }
        this.chosen = this.chosen.map((c) => (c.state === 'edit' ? { ...c, state: 'uploading', skipped: undefined } : c));
        this.error = undefined;

        // What's uploaded is of this session, and once it's done, its
        // folder is loaded again, whether or not the panel is open, or of
        // another folder, which isn't changed of it
        const session = this.session;
        const added = this.added;
        try {
            const result = await uploadObjects(client, this.volume, this.folder, pending.map((c) => ({ file: c.file, path: c.path })), this.overwrite);
            if (result.body.length) {
                added?.();
            }
            if (session !== this.session) {
                return;
            }
            const folder = this.folder === '/' ? '' : this.folder;
            const skipped = new Map((result.skipped ?? []).map((s) => [s.path, s]));
            this.chosen = this.chosen.map((c) => {
                const s = skipped.get(`${folder}/${c.path}`);
                return c.state === 'uploading' ? (s ? { ...c, state: 'edit', skipped: s } : { ...c, state: 'complete' }) : c;
            });
        } catch (error) {
            if (session !== this.session) {
                return;
            }
            this.chosen = this.chosen.map((c) => (c.state === 'uploading' ? { ...c, state: 'edit' } : c));
            this.error = error instanceof Error ? error.message : String(error);
        }
        this.setAction();
    }
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// entryFiles returns the files of an entry which is dropped, of its path,
// of the folder it's in: a file, or every file in a folder, and its
// folders, of their paths, which start with its name
async function entryFiles(entry: FileSystemEntry, folder: string): Promise<{ file: File; path: string }[]> {
    const path = folder ? `${folder}/${entry.name}` : entry.name;
    if (entry.isFile) {
        const file = await new Promise<File>((resolve, reject) => (entry as FileSystemFileEntry).file(resolve, reject));
        return [{ file, path }];
    }
    const reader = (entry as FileSystemDirectoryEntry).createReader();
    const entries: FileSystemEntry[] = [];
    for (;;) {
        // A folder's entries are read in batches, until a batch is empty
        const batch = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
        if (batch.length === 0) {
            break;
        }
        entries.push(...batch);
    }
    return (await Promise.all(entries.map((e) => entryFiles(e, path)))).flat();
}

if (!customElements.get(ObjectUploadTag)) {
    customElements.define(ObjectUploadTag, ObjectUploadElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectUploadTag]: ObjectUploadElement;
    }
}
