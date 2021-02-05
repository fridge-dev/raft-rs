use std::io::ErrorKind;
use std::path::Path;
use std::{fs, io};
use tonic_build;

fn main() -> io::Result<()> {
    build_proto()?;
    Ok(())
}

fn build_proto() -> io::Result<()> {
    idempotent_create_dir("./generated/")?;
    tonic_build::configure()
        .out_dir("./generated/")
        .compile(&["./protos/raft.proto"], &["./protos/"])
}

fn idempotent_create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    match fs::create_dir(path) {
        Ok(_) => Ok(()),
        Err(e) => match e.kind() {
            ErrorKind::AlreadyExists => Ok(()),
            _ => Err(e),
        },
    }
}
