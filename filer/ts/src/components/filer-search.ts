import { LitElement, html, css } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { classMap } from 'lit/directives/class-map.js';
import { FilerClient, SearchResult, ArtworkInfo } from '../api/client.js';

const client = new FilerClient();
const DEBOUNCE_MS = 300;

@customElement('filer-search')
export class FilerSearch extends LitElement {
  static styles = css`
    :host {
      display: block;
      font-family: sans-serif;
      max-width: 720px;
      margin: 2rem auto;
      padding: 0 1rem;
    }
    input {
      width: 100%;
      box-sizing: border-box;
      padding: 0.5rem 0.75rem;
      font-size: 1rem;
      border: 1px solid #ccc;
      border-radius: 4px;
      margin-bottom: 1.5rem;
    }
    .result {
      display: flex;
      gap: 0.75rem;
      align-items: flex-start;
      padding: 0.75rem 0;
      border-bottom: 1px solid #eee;
    }
    .result-artwork {
      flex: 0 0 128px;
      width: 128px;
    }
    .result-artwork img {
      display: block;
      width: 128px;
      height: auto;
      border-radius: 4px;
      object-fit: cover;
    }
    .result-body {
      flex: 1 1 auto;
      min-width: 0;
    }
    .result-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 0.25rem;
    }
    .result-title {
      font-weight: 600;
      cursor: pointer;
      color: #0066cc;
    }
    .result-title:hover {
      text-decoration: underline;
    }
    .result-rank {
      font-size: 0.75rem;
      font-weight: 600;
      color: white;
      background: #0066cc;
      border-radius: 999px;
      padding: 0.1rem 0.5rem;
      white-space: nowrap;
      margin-left: 0.75rem;
    }
    .result-path {
      font-size: 0.85rem;
      color: #555;
      margin-bottom: 0.25rem;
    }
    .result-summary {
      font-size: 0.8rem;
      color: #888;
    }
    .summary {
      font-size: 0.85rem;
      color: #555;
      margin-bottom: 1rem;
    }
    .empty { color: #666; }
    .error { color: #c00; }

    /* Backdrop */
    .backdrop {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.4);
      z-index: 100;
      opacity: 0;
      pointer-events: none;
      transition: opacity 250ms ease;
    }
    .backdrop.open {
      opacity: 1;
      pointer-events: auto;
    }

    /* Drawer */
    .drawer {
      position: fixed;
      top: 0;
      right: 0;
      bottom: 0;
      width: 480px;
      max-width: 90vw;
      background: #fff;
      box-shadow: -2px 0 16px rgba(0, 0, 0, 0.15);
      z-index: 101;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      transform: translateX(100%);
      transition: transform 280ms cubic-bezier(0.4, 0, 0.2, 1);
    }
    .drawer.open {
      transform: translateX(0);
    }
    .drawer-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 1rem 1.25rem;
      border-bottom: 1px solid #eee;
      gap: 0.75rem;
    }
    .drawer-title {
      font-weight: 600;
      font-size: 0.95rem;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .drawer-close {
      flex: 0 0 auto;
      background: none;
      border: none;
      font-size: 1.25rem;
      line-height: 1;
      cursor: pointer;
      color: #555;
      padding: 0.25rem;
    }
    .drawer-close:hover { color: #000; }
    .drawer-body {
      flex: 1 1 auto;
      overflow-y: auto;
      padding: 1.25rem;
    }
    .drawer-body pre {
      margin: 0;
      font-size: 0.8rem;
      font-family: monospace;
      white-space: pre-wrap;
      word-break: break-all;
      color: #222;
    }
  `;

  @state() private query = '';
  @state() private results: SearchResult[] = [];
  @state() private total = 0;
  @state() private error = '';
  @state() private searched = false;
  @state() private selected: SearchResult | null = null;

  private debounceTimer: ReturnType<typeof setTimeout> | undefined;

  private onInput(e: InputEvent) {
    this.query = (e.target as HTMLInputElement).value;
    clearTimeout(this.debounceTimer);
    if (!this.query.trim()) {
      this.results = [];
      this.searched = false;
      return;
    }
    this.debounceTimer = setTimeout(() => this.doSearch(), DEBOUNCE_MS);
  }

  private async doSearch() {
    const q = this.query.trim();
    if (!q) return;
    this.error = '';
    try {
      const list = await client.search({ query: q });
      this.results = list.body ?? [];
      this.total = list.count ?? 0;
      this.searched = true;
    } catch (e) {
      this.error = e instanceof Error ? e.message : 'Search failed';
    }
  }

  private openDrawer(r: SearchResult) {
    this.selected = r;
  }

  private closeDrawer() {
    this.selected = null;
  }

  render() {
    return html`
      <input
        type="search"
        placeholder="Search…"
        .value=${this.query}
        @input=${this.onInput}
      />
      ${this.searched && this.results.length > 0
        ? html`<p class="summary">Showing ${this.results.length} of ${this.total} result${this.total !== 1 ? 's' : ''}</p>`
        : ''}
      ${this.error
        ? html`<p class="error">${this.error}</p>`
        : this.searched && this.results.length === 0
        ? html`<p class="empty">No results found.</p>`
        : this.results.map(r => {
          const art: ArtworkInfo | undefined = r.artwork?.[0];
          return html`
            <div class="result">
              ${art ? html`
                <div class="result-artwork">
                  <img src="/api/artwork/${art.key}" alt="" width="128" />
                </div>
              ` : ''}
              <div class="result-body">
                <div class="result-header">
                  <span class="result-title" @click=${() => this.openDrawer(r)}>${r.title || r.path}</span>
                  <span class="result-rank">${Math.min(r.rank * 100, 100).toFixed(1)}%</span>
                </div>
                ${r.title ? html`<div class="result-path">${r.path}</div>` : ''}
                ${r.summary ? html`<div class="result-summary">${r.summary}</div>` : ''}
              </div>
            </div>
          `;
        })
      }

      <div class=${classMap({ backdrop: true, open: !!this.selected })} @click=${this.closeDrawer}></div>
      <div class=${classMap({ drawer: true, open: !!this.selected })}>
        <div class="drawer-header">
          <span class="drawer-title">${this.selected?.title || this.selected?.path || ''}</span>
          <button class="drawer-close" @click=${this.closeDrawer}>&#x2715;</button>
        </div>
        <div class="drawer-body">
          <pre>${this.selected ? JSON.stringify(this.selected, null, 2) : ''}</pre>
        </div>
      </div>
    `;
  }
}
