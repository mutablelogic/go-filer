// An alert, of what's wrong with what a page shows, which stays while it's
// wrong, rather than a notification, which goes

import { html, nothing, type TemplateResult } from 'lit';
import '@carbon/web-components/es/components/notification/inline-notification.js';

///////////////////////////////////////////////////////////////////////////////
// TYPES

export type AlertKind = 'error' | 'warning' | 'info' | 'success';

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// alertView returns an alert, of its kind, with a title and what's wrong,
// which can't be closed, as it's there for as long as it's wrong
export function alertView(kind: AlertKind, title: string, detail?: string): TemplateResult {
    return html`
        <cds-inline-notification class="gf-alert" kind=${kind} title=${title}
            subtitle=${detail ?? nothing} low-contrast hide-close-button></cds-inline-notification>
    `;
}
