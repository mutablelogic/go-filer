
# Credential Operations

Credentials store secret material - such as API keys or tokens - keyed by a unique
identifier and used internally by other parts of the system (for example, filer
backends) to authenticate against third-party services.

The available operations are create, get, list, delete and rotate:

* Create stores a new credential under a key, encrypting the supplied value.
* Get decrypts and returns the credential value for a key, and requires the
  passphrase used to encrypt it.
* List returns credentials without their values, optionally filtered to those which
  are (or are not) encrypted with the current passphrase version.
* Delete removes a credential by key.
* Rotate re-encrypts a credential with the latest passphrase version, without
  changing its value.

Credential values are encrypted using one or more configured passphrases, each
identified by a passphrase version. Encryption always uses the latest configured
version, while decryption looks up the passphrase version recorded against the
credential. This allows older passphrase versions to remain valid for decrypting
existing credentials even after a new version is introduced.

Rotation re-encrypts a credential with the latest passphrase version, without
changing its value. This is used after introducing a new passphrase version, to
migrate existing credentials away from older (and eventually retired) passphrases.
The list operation can be used to identify which credentials still need rotating.

## GET /credential

List credentials, optionally filtered by passphrase version. The credential values
are never returned, only the key and update timestamp.

## PUT /credential

Create or update a credential. The request body must be a JSON object containing
the key and the credential value to encrypt. If a credential with the same key
already exists its value is replaced. Returns the key and update timestamp of the
stored credential; the credential value itself is never returned.

## GET /credential/{key}

Retrieve and decrypt a credential by key. The passphrase must be supplied in the
request body as `text/plain`. Returns the decrypted credential value as JSON.
Returns 400 if the passphrase does not match any configured version, or 404 if
the key does not exist.

## DELETE /credential/{key}

Delete a credential by key. Returns the key and update timestamp of the deleted
credential. Returns 404 if the key does not exist.

## POST /credential/{key}/rotate

Re-encrypt a credential with the latest configured passphrase version, without
changing its value. Returns the key and update timestamp of the updated credential.
Returns 304 if the credential is already encrypted with the latest passphrase
version, 404 if the key does not exist, or 503 if no passphrase is configured.
