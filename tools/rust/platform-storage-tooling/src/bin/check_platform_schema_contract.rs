use insight_platform_postgres::{generated_schema_contract, validate_checked_in_schema_contract};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn schema_contract_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|candidate| {
            candidate.join("Cargo.toml").is_file()
                && candidate
                    .join("crates/adapters/platform-postgres/Cargo.toml")
                    .is_file()
        })
        .expect("schema tooling runs from the owning workspace")
        .join("crates/adapters/platform-postgres/schema-contract.json")
}

fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let expected = generated_schema_contract();
    if arguments.as_slice() == ["--print"] {
        print!("{}", String::from_utf8_lossy(&expected));
        return;
    }
    if arguments.as_slice() == ["--write"] {
        let path = schema_contract_path();
        fs::write(&path, expected).expect("generated schema contract is writable");
        println!("{} was updated", path.display());
        return;
    }
    if !arguments.is_empty() {
        eprintln!("usage: check-platform-schema-contract [--print|--write]");
        std::process::exit(2);
    }
    if let Err(failure) = validate_checked_in_schema_contract() {
        eprintln!("{failure}");
        std::process::exit(1);
    }
    let path = schema_contract_path();
    match fs::read(&path) {
        Ok(actual) if actual == expected => {
            println!("Platform v1 PostgreSQL schema contract is current")
        }
        Ok(_) => {
            eprintln!(
                "{} differs from the generated schema contract",
                path.display()
            );
            std::process::exit(1);
        }
        Err(failure) => {
            eprintln!("{} cannot be read: {failure}", path.display());
            std::process::exit(1);
        }
    }
}
