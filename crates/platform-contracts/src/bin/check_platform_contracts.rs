use insight_platform_contracts::machine::{
    check_contract_tree, generated_contracts, generated_root_manifest,
    repository_root_from_manifest,
};
use std::fs;

fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let [flag, relative] = arguments.as_slice() {
        if flag == "--print" {
            let contracts = generated_contracts();
            let Some(bytes) = contracts.get(relative.as_str()) else {
                eprintln!("unknown generated contract {relative:?}");
                std::process::exit(2);
            };
            print!("{}", String::from_utf8_lossy(bytes));
            return;
        }
    }
    if arguments.as_slice() == ["--print-manifest"] {
        let bytes = generated_root_manifest(&repository_root_from_manifest())
            .expect("root manifest inputs are readable");
        print!("{}", String::from_utf8_lossy(&bytes));
        return;
    }
    if arguments.as_slice() == ["--write"] {
        let repository_root = repository_root_from_manifest();
        for (relative, bytes) in generated_contracts() {
            let target = repository_root.join("contracts/platform-v1").join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).expect("generated contract parent is writable");
            }
            fs::write(&target, bytes).expect("generated contract is writable");
        }
        let manifest = generated_root_manifest(&repository_root)
            .expect("root manifest inputs are readable after contract generation");
        fs::write(
            repository_root.join("contracts/platform-v1/manifest.json"),
            manifest,
        )
        .expect("root manifest is writable");
        println!("contracts/platform-v1 generated artifacts were updated");
        return;
    }
    if !arguments.is_empty() {
        eprintln!(
            "usage: check-platform-contracts [--print <relative-path> | --print-manifest | --write]"
        );
        std::process::exit(2);
    }
    if let Err(failure) = check_contract_tree(&repository_root_from_manifest()) {
        eprintln!("{failure}");
        std::process::exit(1);
    }
    println!("contracts/platform-v1 generated artifacts are current");
}
