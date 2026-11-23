// The panel which adds files to a folder of a writable volume: files which
// are dropped on it, or chosen, and folders, of every file in them, of
// their paths, so a folder is added in the folder, with its folders, which
// are uploaded into the folder, of one request, replacing those which exist, when it overwrites them, and
// otherwise skipping them, and then which were added, and which skipped,
// and why. A folder's files are listed as they're read, a batch at a time,
// so a large one doesn't hold the page up, and closing the panel, such as
// of Esc, stops reading it, and cancels an upload. open opens it, of the folder, and the function which is called
// once files are added.

import { LitElement, html, nothing, type TemplateResult } from 'lit';
import { repeat } from 'lit/directives/repeat.js';
import type { Client } from '@mutablelogic/go-server/api';
import { PanelController, type PanelAction } from '@mutablelogic/go-server/panel';
import '@mutablelogic/go-server/panel/view';
import Upload16 from '@carbon/icons/es/upload/16.js';
import '@carbon/web-components/es/components/file-uploader/index.js';
import '@carbon/web-components/es/components/toggle/index.js';
import '@carbon/web-components/es/components/button/index.js';
import '@carbon/web-components/es/components/layer/index.js';
import { alertView } from '../alert/view.ts';
import { uploadObjects } from '../filer/httpclient.ts';
import type { ObjectSkipped } from '../filer/schema.ts';
import { formatSize, skippedFirst } from './format.ts';

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

// MaxListed is how many of the files which are chosen are listed, of more,
// which are counted, and BatchSize how many of a folder's files are added
// at once, as it's read
const MaxListed = 100;
const BatchSize = 200;

// FrameBudget is how long, in milliseconds, a folder is read, between the
// page being drawn, at most
const FrameBudget = 50;

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class ObjectUploadElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        chosen: { state: true },
        reading: { state: true },
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

    // reading is how many folders are being read, whose files are added
    // as they are
    declare private reading: number;

    private volume = '';
    private folder = '/';
    private added?: () => void;
    private readonly panel = new PanelController<string>({ closable: true });

    // session is of each time the panel is opened, and closed, so what's
    // read, or uploaded, of one before, isn't of this one, paths are those
    // which are chosen, and abort cancels the upload in progress
    private session = 0;
    private paths = new Set<string>();
    private abort?: AbortController;

    constructor() {
        super();
        this.chosen = [];
        this.overwrite = false;
        this.reading = 0;
        this.panel.onClose(() => this.cancel());
    }

    // cancel stops reading folders, and the upload in progress, once the
    // panel is closed, or opened again
    private cancel() {
        this.session++;
        this.abort?.abort();
        this.abort = undefined;
        this.reading = 0;
    }

    protected override createRenderRoot() {
        return this;
    }

    // open opens the panel of the volume's folder, and calls the function
    // once files are added to it
    open(volume: string, folder: string, added?: () => void) {
        this.cancel();
        this.volume = volume;
        this.folder = folder;
        this.added = added;
        this.chosen = [];
        this.paths = new Set();
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
    // their sizes, and whether those which exist are replaced, of the layer
    // above the panel's, as Carbon's are, so each is of a box of its own
    private formView(): TemplateResult {
        const uploading = this.chosen.some((c) => c.state === 'uploading');
        const done = this.chosen.length > 0 && this.chosen.every((c) => c.state === 'complete');
        return html`
            <div class="gf-volume-form">
                <cds-layer level="1">
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
                        ${repeat(skippedFirst(this.chosen, MaxListed), (c) => c.path, (c) => html`
                            <cds-file-uploader-item state=${c.state} ?invalid=${!!c.skipped}
                                error-subject=${c.skipped ? (c.skipped.exists ? 'It exists' : "It wasn't added") : ''}
                                error-body=${c.skipped?.reason ?? ''}
                                @cds-file-uploader-item-deleted=${(event: Event) => { event.preventDefault(); this.unchoose(c.path); }}>
                                ${c.path} <span class="gf-object-upload-size">${formatSize(c.file.size)}</span>
                            </cds-file-uploader-item>
                        `)}
                    </cds-file-uploader>
                </cds-layer>
                ${this.summaryView()}
                ${done ? nothing : html`
                    <cds-toggle size="sm" label-text="Replace files which exist" label-a="Yes" label-b="No" ?checked=${this.overwrite} ?disabled=${uploading}
                        @cds-toggle-changed=${(event: CustomEvent<{ checked: boolean }>) => { this.overwrite = event.detail.checked; }}></cds-toggle>
                `}
                ${this.error ? alertView('error', "The files couldn't be added", this.error) : nothing}
            </div>
        `;
    }

    // summaryView says how many files are chosen, and their size, beneath
    // those which are listed, of more than are, and that a folder is being
    // read, while it is
    private summaryView(): TemplateResult | typeof nothing {
        const more = this.chosen.length - MaxListed;
        if (more <= 0 && !this.reading) {
            return nothing;
        }
        const size = this.chosen.reduce((total, c) => total + c.file.size, 0);
        const skipped = this.chosen.filter((c) => c.skipped).length;
        const count = `${this.chosen.length.toLocaleString()} ${this.chosen.length === 1 ? 'file' : 'files'}, ${formatSize(size) || '0 B'}`;
        return html`
            <p class="gf-object-upload-summary">
                ${more > 0 ? `and ${more.toLocaleString()} more, of ${count}` : count}${skipped ? ` · ${skipped.toLocaleString()} not added` : ''}${this.reading ? ' · Reading…' : ''}
            </p>
        `;
    }

    // add adds files which are dropped, or chosen, of their paths, other
    // than those of the same paths, which are chosen already
    private add(files: readonly { file: File; path: string }[]) {
        const added: Chosen[] = [];
        for (const f of files) {
            if (!this.paths.has(f.path)) {
                this.paths.add(f.path);
                added.push({ ...f, state: 'edit' });
            }
        }
        if (added.length) {
            this.chosen = [...this.chosen, ...added];
        }
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
        void this.read(entries);
    }

    // read adds the files of what's dropped, of their paths, a batch at a
    // time, as they're read, until they're all read, or the panel is closed
    private async read(entries: readonly FileSystemEntry[]) {
        const session = this.session;
        const live = () => session === this.session;
        this.reading++;
        this.setAction();
        // Batches are added together, once a frame's time is spent, rather
        // than of each, so a tree of small folders isn't a frame of each
        let pending: { file: File; path: string }[] = [];
        let since = performance.now();
        const flush = async () => {
            this.add(pending);
            pending = [];
            await nextFrame();
            since = performance.now();
        };
        try {
            for (const entry of entries) {
                for await (const batch of entryBatches(entry, '', live)) {
                    if (!live()) {
                        return;
                    }
                    pending.push(...batch);
                    if (pending.length >= BatchSize || performance.now() - since > FrameBudget) {
                        await flush();
                    }
                }
            }
            if (live() && pending.length) {
                this.add(pending);
            }
        } catch (error) {
            if (live()) {
                this.error = `The folder couldn't be read: ${error instanceof Error ? error.message : String(error)}`;
            }
        } finally {
            if (live()) {
                this.reading--;
                this.setAction();
            }
        }
    }

    // chooseFolder adds the files of a folder which is chosen, of their
    // paths, which start with the folder's name, a batch at a time
    private async chooseFolder(input: HTMLInputElement) {
        const files = [...(input.files ?? [])];
        input.value = '';
        const session = this.session;
        this.reading++;
        this.setAction();
        for (let i = 0; i < files.length && session === this.session; i += BatchSize) {
            this.add(files.slice(i, i + BatchSize).map((file) => ({ file, path: file.webkitRelativePath || file.name })));
            await nextFrame();
        }
        if (session === this.session) {
            this.reading--;
            this.setAction();
        }
    }

    // unchoose removes a file which is chosen, of its path
    private unchoose(path: string) {
        this.paths.delete(path);
        this.chosen = this.chosen.filter((c) => c.path !== path);
        this.setAction();
    }

    // setAction sets the panel's action: Add, of the files which are chosen,
    // or Done, once they're added
    private setAction() {
        const done = this.chosen.length > 0 && this.chosen.every((c) => c.state === 'complete');
        const action: PanelAction = done
            ? { label: 'Done', kind: 'primary' }
            : { label: 'Add', kind: 'primary', icon: Upload16, close: false, disabled: this.chosen.length === 0 || this.reading > 0, run: () => this.upload() };
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
        const abort = new AbortController();
        this.abort = abort;
        try {
            const result = await uploadObjects(client, this.volume, this.folder, pending.map((c) => ({ file: c.file, path: c.path })), this.overwrite, abort.signal);
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
            // An upload which is cancelled, as the panel is closed, isn't
            // an error
            if (session !== this.session || abort.signal.aborted) {
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

// entryBatches returns the files of an entry which is dropped, of its
// path, of the folder it's in, a batch at a time, as they're read: a file,
// or every file in a folder, and its folders, of their paths, which start
// with its name, until there are no more, or it isn't live
async function* entryBatches(entry: FileSystemEntry, folder: string, live: () => boolean): AsyncGenerator<{ file: File; path: string }[]> {
    const path = folder ? `${folder}/${entry.name}` : entry.name;
    if (entry.isFile) {
        yield [{ file: await entryFile(entry as FileSystemFileEntry), path }];
        return;
    }

    // A folder's entries are read in batches, until a batch is empty, of
    // which its files are, and its folders, in turn
    const reader = (entry as FileSystemDirectoryEntry).createReader();
    while (live()) {
        const entries = await new Promise<FileSystemEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
        if (entries.length === 0) {
            return;
        }
        const files = entries.filter((e) => e.isFile) as FileSystemFileEntry[];
        for (let i = 0; i < files.length && live(); i += BatchSize) {
            yield await Promise.all(files.slice(i, i + BatchSize).map(async (e) => ({ file: await entryFile(e), path: `${path}/${e.name}` })));
        }
        for (const sub of entries.filter((e) => e.isDirectory)) {
            if (!live()) {
                return;
            }
            yield* entryBatches(sub, path, live);
        }
    }
}

// entryFile returns the file of a file entry
function entryFile(entry: FileSystemFileEntry): Promise<File> {
    return new Promise<File>((resolve, reject) => entry.file(resolve, reject));
}

// nextFrame resolves once the page is drawn, so what's added is shown,
// and the page answers, between batches
function nextFrame(): Promise<void> {
    return new Promise((resolve) => requestAnimationFrame(() => resolve()));
}

if (!customElements.get(ObjectUploadTag)) {
    customElements.define(ObjectUploadTag, ObjectUploadElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectUploadTag]: ObjectUploadElement;
    }
}
