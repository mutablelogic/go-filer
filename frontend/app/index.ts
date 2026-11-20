// The filer server's web UI, which is composed of modules, each of which
// owns an item in the menu bar, and the sidebar while it's active: Files,
// of the volumes and their objects. Another app includes a module the same
// way, from @mutablelogic/go-filer/files/view.

import '@carbon/styles/css/styles.min.css';
import './fonts.css';

// go-server's components
import { ChangesController } from '@mutablelogic/go-server/broadcaster';
import { ConfigController, configClient, getConfig } from '@mutablelogic/go-server/config';
import '@mutablelogic/go-server/config/view';
import { NavController } from '@mutablelogic/go-server/nav';
import '@mutablelogic/go-server/nav/view';
import { NotificationController } from '@mutablelogic/go-server/notification';
import '@mutablelogic/go-server/notification/view';
import { Router } from '@mutablelogic/go-server/router';

// go-filer's modules
import { filesModule } from '../src/files/view.ts';

// Errors, and the server's status, are shown as toasts
const notifications = new NotificationController();
const notificationsView = document.querySelector('gs-notifications');
if (notificationsView) {
    notificationsView.controller = notifications;
}

// The configuration says where the API is, and the changes, which the
// server's status follows
const read = await getConfig().catch((error) => {
    notifications.error("The configuration couldn't be read", error);
    return undefined;
});
const client = configClient(read ?? { prefix: '/api' });
const changes = new ChangesController(client).onError((error) => notifications.error("Changes couldn't be watched", error));
const server = new ConfigController(changes, { config: read, notifications });
const status = document.querySelector('gs-server-status');
if (status) {
    status.controller = server;
    status.repository = 'https://github.com/mutablelogic/go-filer';
}

// The navigation, of the modules' sections, and the router of their pages
const nav = new NavController({ name: 'filer', prefix: 'go', href: '#', pages: [] });
const header = document.querySelector('gs-header');
const sideNav = document.querySelector('gs-side-nav');
if (header && sideNav) {
    header.controller = nav;
    sideNav.controller = nav;
}
const router = new Router('files');
const root = document.querySelector('main')!;

// The modules
const app = { nav, router, client, root, notifications, changes };
filesModule(app);

// Show the page of the location's hash, and follow the server
router.onRoute((route) => {
    window.scrollTo(0, 0);
    nav.setActive(route.page);
}).start();
changes.start();
