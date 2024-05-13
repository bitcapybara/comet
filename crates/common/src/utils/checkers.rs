use std::sync::OnceLock;

static RE: OnceLock<regex::Regex> = OnceLock::new();

pub fn is_valid_name(name: &str) -> bool {
    let re = RE.get_or_init(|| regex::Regex::new("^[a-zA-Z][a-zA-Z0-9_]*$").unwrap());
    re.is_match(name)
}
