use std::env;
use std::path::{Path, PathBuf};

const PROGRAMS: &[(&str, &str)] = &[
    ("policy", "../../tls_policy.c"),
    ("client", "../client.c"),
    ("cert_buffers", "../cert_buffers.c"),
    ("fips_cert_buffers", "../fips_cert_buffers.c"),
    ("handshake_timeout", "../handshake_timeout.c"),
    ("no_provider", "../no_provider.c"),
];

fn compile(name: &str, path: &Path, includes: &[PathBuf]) {
    println!("cargo:rerun-if-changed={}", path.display());
    cc::Build::new()
        .file(path)
        .includes(includes)
        .define("main", format!("rdk_{name}_main").as_str())
        .warnings(false)
        .compile(&format!("rdk_{name}"));
}

fn main() {
    let here = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let src = here.join("../../../src");
    for (name, file) in PROGRAMS {
        compile(name, &here.join(file), &[src.clone()]);
    }

    // The whitebox transport test compiles rdkafka_rustls.c itself, so it
    // needs the build's generated config and the linked rustls-ffi header.
    println!("cargo:rerun-if-env-changed=RDKAFKA_CONFIG_DIR");
    if env::var("CARGO_CFG_UNIX").is_ok() {
        let config = env::var("RDKAFKA_CONFIG_DIR")
            .expect("set RDKAFKA_CONFIG_DIR to the CMake build's generated/dummy directory");
        let rustls = env::var("DEP_RUSTLS_FFI_INCLUDE").unwrap();
        let includes = [PathBuf::from(config), src.clone(), PathBuf::from(rustls)];
        compile("transport_io", &here.join("../transport_io.c"), &includes);
    }

    // `pkg-config --static --libs rdkafka-static` of the installed library.
    // Emitting librdkafka as a link-lib puts it before the rustls-ffi rlib,
    // which order-sensitive linkers need.
    println!("cargo:rerun-if-env-changed=RDKAFKA_STATIC_LIBS");
    let libs = env::var("RDKAFKA_STATIC_LIBS")
        .expect("set RDKAFKA_STATIC_LIBS to `pkg-config --static --libs rdkafka-static`");
    let mut tokens = libs.split_whitespace();
    while let Some(token) = tokens.next() {
        if let Some(dir) = token.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={dir}");
        } else if let Some(lib) = token.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={lib}");
        } else if token == "-framework" {
            println!("cargo:rustc-link-lib=framework={}", tokens.next().unwrap());
        } else if token.ends_with(".a") || token.ends_with(".lib") {
            let path = Path::new(token);
            let name = path.file_stem().unwrap().to_str().unwrap();
            let name = name.strip_prefix("lib").filter(|_| token.ends_with(".a")).unwrap_or(name);
            println!("cargo:rustc-link-search=native={}", path.parent().unwrap().display());
            println!("cargo:rustc-link-lib=static={name}");
        } else {
            println!("cargo:rustc-link-arg-bins={token}");
        }
    }
}
