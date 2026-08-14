use insight_platform_postgres::{generated_schema_contract, validate_checked_in_schema_contract};
use std::{fs, path::Path};

fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let expected = generated_schema_contract();
    if arguments.as_slice() == ["--print"] {
        print!("{}", String::from_utf8_lossy(&expected));
        return;
    }
    if arguments.as_slice() == ["--write"] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("schema-contract.json");
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
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("schema-contract.json");
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
