use std::path::PathBuf;

#[derive(Clone)]
pub struct CertsFile {
    pub ca_cert_file: PathBuf,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}
