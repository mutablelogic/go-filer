
# Credential Operations

Credentials store secret material - such as API keys or tokens - keyed by a unique
identifier and used internally by other parts of the system (for example, filer
backends) to authenticate against third-party services. The credential value itself
is never returned once written: it is encrypted at rest, and only the key and update
timestamp are ever exposed through the API.

The available operations are create, get, list, delete and rotate. Create stores a
new credential under a key, encrypting the supplied value. Get decrypts and returns
the credential value for a key, and requires the passphrase used to encrypt it. List
returns credentials without their values, optionally filtered to those which are (or
are not) encrypted with the current passphrase version. Delete removes a credential
by key.

Credential values are encrypted using one or more configured passphrases, each
identified by a passphrase version. Encryption always uses the latest configured
version, while decryption looks up the passphrase version recorded against the
credential. This allows older passphrase versions to remain valid for decrypting
existing credentials even after a new version is introduced.

Rotation re-encrypts a credential with the latest passphrase version, without
changing its value. This is used after introducing a new passphrase version, to
migrate existing credentials away from older (and eventually retired) passphrases.
The list operation can be used to identify which credentials still need rotating.
