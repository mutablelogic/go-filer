// The gf-object-preview element is a file's content, as Markdown, which the
// server reads it as, such as a PDF's, rendered as HTML, which is
// sanitized, without images, or styles, so a document, such as an email,
// doesn't load what's elsewhere. It's loaded once it's shown, and again
// when the file changes, and says when only its start is shown, or it can't
// be previewed.

import { LitElement, css, html, nothing, type PropertyValues } from 'lit';
import { unsafeHTML } from 'lit/directives/unsafe-html.js';
import type { Client } from '@mutablelogic/go-server/api';
import { marked } from 'marked';
import DOMPurify from 'dompurify';
import '@carbon/web-components/es/components/skeleton-text/index.js';
import { readMarkdown } from '../filer/httpclient.ts';
import type { FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// PreviewState is the preview: loading, of HTML, of a file which can't be
// previewed, or which couldn't be loaded, and why
type PreviewState =
    | { kind: 'loading' }
    | { kind: 'loaded'; html: string; more: boolean }
    | { kind: 'unavailable' }
    | { kind: 'failed'; error: string };

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const ObjectPreviewTag = 'gf-object-preview';

// Links open in a new tab, without the page they're opened from
DOMPurify.addHook('afterSanitizeAttributes', (node) => {
    if (node instanceof HTMLAnchorElement) {
        node.setAttribute('target', '_blank');
        node.setAttribute('rel', 'noopener noreferrer');
    }
});

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

export class ObjectPreviewElement extends LitElement {
    static override properties = {
        client: { attribute: false },
        object: { attribute: false },
        state: { state: true },
    };

    // client is the server's, and object the file which is previewed
    declare client?: Client;
    declare object?: FilerObject;
    declare private state: PreviewState;

    // loaded is what was loaded, of the file's path and version, so it isn't
    // loaded again until it changes, and abort cancels what's loading
    private loaded?: string;
    private abort?: AbortController;

    static override styles = css`
        :host {
            display: block;
            font-size: 0.875rem;
        }

        .note {
            margin: 0 0 1rem;
            color: var(--cds-text-secondary);
            font-style: italic;
        }

        .markdown {
            line-height: 1.5;
            overflow-wrap: anywhere;
        }

        .markdown > :first-child {
            margin-top: 0;
        }

        .markdown h1, .markdown h2, .markdown h3 {
            margin: 1.25rem 0 0.5rem;
            font-weight: 600;
            line-height: 1.3;
        }

        .markdown h1 { font-size: 1.25rem; }
        .markdown h2 { font-size: 1.125rem; }
        .markdown h3 { font-size: 1rem; }

        .markdown p, .markdown ul, .markdown ol, .markdown pre, .markdown table, .markdown blockquote {
            margin: 0 0 0.75rem;
        }

        .markdown ul, .markdown ol {
            padding-left: 1.5rem;
        }

        .markdown hr {
            margin: 1rem 0;
            border: 0;
            border-top: 1px solid var(--cds-border-subtle-01, #e0e0e0);
        }

        .markdown a {
            color: var(--cds-link-primary, #0f62fe);
        }

        .markdown code {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.8125rem;
        }

        .markdown pre {
            padding: 0.75rem;
            overflow-x: auto;
            background: var(--cds-layer-02, #fff);
        }

        .markdown blockquote {
            padding-left: 0.75rem;
            border-left: 3px solid var(--cds-border-subtle-01, #e0e0e0);
            color: var(--cds-text-secondary);
        }

        .markdown table {
            border-collapse: collapse;
        }

        .markdown th, .markdown td {
            padding: 0.25rem 0.5rem;
            border: 1px solid var(--cds-border-subtle-01, #e0e0e0);
            text-align: left;
        }
    `;

    constructor() {
        super();
        this.state = { kind: 'loading' };
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.abort?.abort();
        this.abort = undefined;
        this.loaded = undefined;
    }

    // The file is loaded once it's shown, and again, when it changes
    protected override willUpdate(changed: PropertyValues<this>) {
        if ((changed.has('object') || changed.has('client')) && this.object && this.client) {
            const version = [this.object.name, this.object.path, this.object.etag ?? '', this.object.mtime ?? '', this.object.size ?? ''].join('\n');
            if (version !== this.loaded) {
                this.loaded = version;
                void this.load(this.client, this.object);
            }
        }
    }

    protected override render() {
        const state = this.state;
        switch (state.kind) {
            case 'loading':
                return html`<cds-skeleton-text paragraph line-count="6" aria-busy="true"></cds-skeleton-text>`;
            case 'unavailable':
                return html`<p class="note">This file can't be previewed</p>`;
            case 'failed':
                return html`<p class="note">${state.error}</p>`;
            case 'loaded':
                return html`
                    ${state.more ? html`<p class="note">Only the beginning is shown</p>` : nothing}
                    <div class="markdown">${unsafeHTML(state.html)}</div>
                `;
        }
    }

    // load reads the file as Markdown, in place of what's loading, and
    // renders it, which is sanitized
    private async load(client: Client, object: FilerObject) {
        this.abort?.abort();
        const abort = new AbortController();
        this.abort = abort;
        this.state = { kind: 'loading' };
        try {
            const part = await readMarkdown(client, object.name, object.path, abort.signal);
            if (abort.signal.aborted) {
                return;
            } else if (!part) {
                this.state = { kind: 'unavailable' };
                return;
            }
            const rendered = marked.parse(part.text, { async: false, gfm: true });
            const sanitized = DOMPurify.sanitize(rendered, { FORBID_TAGS: ['img', 'style', 'picture', 'source', 'video', 'audio', 'iframe'], FORBID_ATTR: ['style'] });
            this.state = { kind: 'loaded', html: sanitized, more: part.more };
        } catch (error) {
            if (!abort.signal.aborted) {
                this.state = { kind: 'failed', error: error instanceof Error ? error.message : String(error) };
            }
        }
    }
}

if (!customElements.get(ObjectPreviewTag)) {
    customElements.define(ObjectPreviewTag, ObjectPreviewElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [ObjectPreviewTag]: ObjectPreviewElement;
    }
}
