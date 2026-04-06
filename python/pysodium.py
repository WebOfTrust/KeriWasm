"""pysodium compatibility shim for Pyodide/WASM environments.

keripy imports ``pysodium`` for libsodium bindings.  In Pyodide the native
C-extension wheel cannot load — instead we ship ``pychloride`` (a WASM-compiled
libsodium wheel).  This shim re-exports the public API so that
``import pysodium`` works transparently in the browser.

Only the subset used by keripy is forwarded. Add more as needed.
"""
from pychloride import (  # noqa: F401 — re-export
    crypto_sign_keypair,
    crypto_sign_detached,
    crypto_sign_verify_detached,
    # pychloride uses shortened names for the ed25519↔curve25519 converters;
    # pysodium (and keripy/locksmith) expect the full libsodium names.
    crypto_sign_pk_to_box_pk as crypto_sign_ed25519_pk_to_curve25519,
    crypto_sign_sk_to_box_sk as crypto_sign_ed25519_sk_to_curve25519,
    crypto_sign_seed_keypair,
    crypto_sign_BYTES,
    crypto_sign_PUBLICKEYBYTES,
    crypto_sign_SECRETKEYBYTES,
    crypto_sign_SEEDBYTES,
    crypto_generichash,
    crypto_pwhash_SALTBYTES,
    crypto_pwhash,
    crypto_pwhash_ALG_ARGON2ID13,
    crypto_pwhash_MEMLIMIT_INTERACTIVE,
    crypto_pwhash_OPSLIMIT_INTERACTIVE,
    crypto_aead_xchacha20poly1305_ietf_encrypt,
    crypto_aead_xchacha20poly1305_ietf_decrypt,
    crypto_aead_xchacha20poly1305_ietf_KEYBYTES,
    crypto_aead_xchacha20poly1305_ietf_NPUBBYTES,
    randombytes,
)
