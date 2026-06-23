import { LitElement, html, css } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { FilerClient, SearchResult } from '../api/client.js';

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
      padding: 0.75rem 0;
      border-bottom: 1px solid #eee;
    }
    .result-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 0.25rem;
    }
    .result-title { font-weight: 600; }
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
  `;

  @state() private query = '';
  @state() private results: SearchResult[] = [];
  @state() private total = 0;
  @state() private error = '';
  @state() private searched = false;

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
        : this.results.map(r => html`
          <div class="result">
            <div class="result-header">
              <span class="result-title">${r.title || r.path}</span>
              <span class="result-rank">${(r.rank * 100).toFixed(1)}%</span>
            </div>
            ${r.title ? html`<div class="result-path">${r.path}</div>` : ''}
            ${r.summary ? html`<div class="result-summary">${r.summary}</div>` : ''}
          </div>
        `)
      }
    `;
  }
}
