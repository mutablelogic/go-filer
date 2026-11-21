// Signing in, in a browser, when a volume's user needs to, such as with
// Google, which the server says, when a volume is added, or mounted: an
// alert which asks them to, and the panel's action, which opens a window at
// the URL to sign in at, whose callback posts a message, once they have

import type { TemplateResult } from 'lit';
import type { Client } from '@mutablelogic/go-server/api';
import { alertView } from '../alert/view.ts';
import { authorizeSignIn, signInCallback } from '../filer/httpclient.ts';
import type { SignInNeeded } from './create.ts';

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SignInLabel is the label of the panel's action which signs in
export const SignInLabel = 'Sign in with Google';

// signInPrompt asks the volume's user to sign in, of what's done once they
// have, such as "it's added", of the panel's action
export function signInPrompt(done: string): TemplateResult {
    return alertView('info', 'Sign in with Google', `The volume needs a sign-in. Sign in, in the window which opens, and ${done}.`);
}

// openSignIn opens a window, at once, as it's of a click, which a browser
// allows, and then at the URL to sign in at, which the server makes, or
// closes it, and throws why it can't, such as when the server's client isn't
// a web application's, of the CLI's command which signs in instead
export async function openSignIn(client: Client, signIn: SignInNeeded, command: string): Promise<void> {
    const popup = window.open('', 'gf-signin', 'popup,width=520,height=680');
    try {
        const url = await authorizeSignIn(client, signIn.key, signIn.scopes);
        if (popup) {
            popup.location.href = url;
        } else {
            window.location.href = url;
        }
    } catch (error) {
        popup?.close();
        throw new Error(`${error instanceof Error ? error.message : String(error)}. Signing in in a browser needs the server's GOOGLE_CLIENT to be a web application's client, whose redirect URIs are this server's callback, ${signInCallback(client)}, so sign in with the CLI: ${command}`);
    }
}
