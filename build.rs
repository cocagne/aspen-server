use std::process::Command;
use std::env;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    let schema = "../amoeba/schema/network_protocol.fbs";

    Command::new("flatc").args(&["--rust", "-o", "src", schema])
                       .status().unwrap();

    //println!("cargo:rustc-link-search=native={}", out_dir);
    println!("cargo:rustc-link-lib=static=flatbuffers");
    println!("cargo:rerun-if-changed={}", &schema);
    println!("Output directory: {}", &out_dir);
}