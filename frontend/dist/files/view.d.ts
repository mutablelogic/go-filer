import { LitElement } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import type { ChangesController } from '@mutablelogic/go-server/broadcaster';
import type { NavController } from '@mutablelogic/go-server/nav';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import type { Router } from '@mutablelogic/go-server/router';
import '@mutablelogic/go-server/page/view';
export declare const FilesTag = "gf-files";
export declare class FilesElement extends LitElement {
    static properties: {
        client: {
            attribute: boolean;
        };
        notifications: {
            attribute: boolean;
        };
        changes: {
            attribute: boolean;
        };
        volumes: {
            state: boolean;
        };
    };
    client?: Client;
    notifications?: NotificationController;
    changes?: ChangesController;
    private volumes?;
    private unsubscribe?;
    private abort?;
    protected createRenderRoot(): this;
    connectedCallback(): void;
    disconnectedCallback(): void;
    load(): Promise<void>;
    render(): import("lit-html").TemplateResult<1>;
}
declare global {
    interface HTMLElementTagNameMap {
        [FilesTag]: FilesElement;
    }
}
export interface FilesApp {
    nav: NavController;
    router: Router;
    client: Client;
    root: HTMLElement;
    notifications?: NotificationController;
    changes?: ChangesController;
}
export declare function filesModule(app: FilesApp, section?: string): FilesElement;
//# sourceMappingURL=view.d.ts.map