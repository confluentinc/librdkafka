//! Installs a rustls process-default provider, then runs one of the C test
//! programs against static librdkafka. Usage: HOST PROGRAM [ARGS...]
//! The no_provider program runs without an installed provider.
use std::ffi::{c_char, c_int, CString};
use std::process::exit;

use rustls::crypto::CryptoProvider;
use rustls_ffi as _;

type Entry = unsafe extern "C" fn(c_int, *mut *mut c_char) -> c_int;

extern "C" {
    fn rdk_policy_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_client_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_cert_buffers_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_fips_cert_buffers_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_handshake_timeout_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
    fn rdk_no_provider_main(argc: c_int, argv: *mut *mut c_char) -> c_int;
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let entry: Entry = match args.first().map(String::as_str) {
        Some("policy") => rdk_policy_main,
        Some("client") => rdk_client_main,
        Some("cert_buffers") => rdk_cert_buffers_main,
        Some("fips_cert_buffers") => rdk_fips_cert_buffers_main,
        Some("handshake_timeout") => rdk_handshake_timeout_main,
        Some("no_provider") => rdk_no_provider_main,
        #[cfg(unix)]
        Some("transport_io") => rdk_transport_io_main,
        _ => {
            eprintln!("usage: HOST PROGRAM [ARGS...]");
            exit(2);
        }
    };
    if args[0] != "no_provider" {
        let (name, provider) = provider();
        eprintln!("host: process-default provider {name}, fips={}", provider.fips());
        provider
            .install_default()
            .expect("process-default provider already installed");
    }
    let owned: Vec<CString> = args.iter().map(|a| CString::new(a.as_str()).unwrap()).collect();
    let mut argv: Vec<*mut c_char> = owned.iter().map(|a| a.as_ptr() as *mut c_char).collect();
    argv.push(std::ptr::null_mut());
    exit(unsafe { entry(owned.len() as c_int, argv.as_mut_ptr()) });
}
