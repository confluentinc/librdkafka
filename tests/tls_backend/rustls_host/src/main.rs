//! Installs a rustls process-default provider, then runs one of the C test
//! programs against static librdkafka.
//! Usage: HOST [--opaque-keys] [--empty-provider] [--install-provider]
//!             PROGRAM [ARGS...]
//! no_provider runs without a provider unless --empty-provider or
//! --install-provider is given.
//! --opaque-keys hides the public key of loaded keys, like CNG keys do.
//! --empty-provider removes all cipher suites and key exchange groups.
use std::ffi::{c_char, c_int, CString};
use std::process::exit;
use std::sync::Arc;

use rustls::crypto::{CryptoProvider, KeyProvider};
use rustls::pki_types::PrivateKeyDer;
use rustls::sign::{Signer, SigningKey};
use rustls::{Error, SignatureAlgorithm, SignatureScheme};
use rustls_ffi as _;

type Entry = unsafe extern "C" fn(c_int, *mut *mut c_char) -> c_int;

extern "C" {
    fn rdk_policy_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_client_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_cert_buffers_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_fips_cert_buffers_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_handshake_timeout_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_no_provider_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_key_match_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    #[cfg(unix)]
    fn rdk_transport_io_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
}

#[cfg(all(windows, feature = "cng-fips"))]
fn provider() -> (&'static str, CryptoProvider) {
    ("rustls-cng-crypto FIPS", rustls_cng_crypto::fips_provider())
}

#[cfg(all(windows, feature = "cng", not(feature = "cng-fips")))]
fn provider() -> (&'static str, CryptoProvider) {
    ("rustls-cng-crypto", rustls_cng_crypto::default_provider())
}

#[cfg(all(feature = "fips", not(all(windows, feature = "cng"))))]
fn provider() -> (&'static str, CryptoProvider) {
    ("aws-lc-rs FIPS", rustls::crypto::default_fips_provider())
}

#[cfg(not(any(feature = "fips", all(windows, feature = "cng"))))]
fn provider() -> (&'static str, CryptoProvider) {
    ("aws-lc-rs", rustls::crypto::aws_lc_rs::default_provider())
}

#[derive(Debug)]
struct OpaqueKeys(&'static dyn KeyProvider);

#[derive(Debug)]
struct OpaqueKey(Arc<dyn SigningKey>);

impl KeyProvider for OpaqueKeys {
    fn load_private_key(&self, der: PrivateKeyDer<'static>) -> Result<Arc<dyn SigningKey>, Error> {
        Ok(Arc::new(OpaqueKey(self.0.load_private_key(der)?)))
    }
    fn fips(&self) -> bool {
        self.0.fips()
    }
}

// public_key() keeps the trait default (None), so keys_match() is Unknown.
impl SigningKey for OpaqueKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
        self.0.choose_scheme(offered)
    }
    fn algorithm(&self) -> SignatureAlgorithm {
        self.0.algorithm()
    }
}

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut opaque_keys = false;
    let mut empty_provider = false;
    let mut install_provider = false;
    while let Some(flag) = args.first().filter(|a| a.starts_with("--")).cloned() {
        match flag.as_str() {
            "--opaque-keys" => opaque_keys = true,
            "--empty-provider" => empty_provider = true,
            "--install-provider" => install_provider = true,
            _ => {
                eprintln!("unknown option {flag}");
                exit(2);
            }
        }
        args.remove(0);
    }
    let entry: Entry = match args.first().map(String::as_str) {
        Some("policy") => rdk_policy_main,
        Some("client") => rdk_client_main,
        Some("cert_buffers") => rdk_cert_buffers_main,
        Some("fips_cert_buffers") => rdk_fips_cert_buffers_main,
        Some("handshake_timeout") => rdk_handshake_timeout_main,
        Some("no_provider") => rdk_no_provider_main,
        Some("key_match") => rdk_key_match_main,
        #[cfg(unix)]
        Some("transport_io") => rdk_transport_io_main,
        _ => {
            eprintln!("usage: HOST PROGRAM [ARGS...]");
            exit(2);
        }
    };
    if args[0] != "no_provider" || empty_provider || install_provider {
        let (name, mut provider) = provider();
        if opaque_keys {
            provider.key_provider = Box::leak(Box::new(OpaqueKeys(provider.key_provider)));
        }
        if empty_provider {
            provider.cipher_suites.clear();
            provider.kx_groups.clear();
        }
        eprintln!(
            "host: process-default provider {name}, fips={}, opaque_keys={opaque_keys}, empty={empty_provider}",
            provider.fips()
        );
        provider
            .install_default()
            .expect("process-default provider already installed");
    }
    let owned: Vec<CString> = args.iter().map(|a| CString::new(a.as_str()).unwrap()).collect();
    let mut argv: Vec<*mut c_char> = owned.iter().map(|a| a.as_ptr() as *mut c_char).collect();
    argv.push(std::ptr::null_mut());
    exit(unsafe { entry(owned.len() as c_int, argv.as_mut_ptr()) });
}
