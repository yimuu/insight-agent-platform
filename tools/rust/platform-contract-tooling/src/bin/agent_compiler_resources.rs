//! Bounded local measurement of the actual native authoring byte boundary.
use insight_platform_agent_compiler::{
    compile_authoring_request_bytes, compiler_semantic_identity, inspect_source_request_bytes,
    MAX_AGENT_COMPILER_REQUEST_BYTES, MAX_AGENT_SOURCE_FILE_BYTES,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, time::Instant};

fn digest(bytes: &[u8]) -> String {
    let hex = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{hex}")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--limits"] {
        println!(
            "{}",
            json!({
                "schema_version": 1,
                "compiler_semantic_identity": compiler_semantic_identity(),
                "maximum_request_bytes": MAX_AGENT_COMPILER_REQUEST_BYTES,
                "maximum_source_file_bytes": MAX_AGENT_SOURCE_FILE_BYTES,
            })
        );
        return Ok(());
    }
    if args.len() != 3 {
        return Err(
            "usage: agent_compiler_resources [--limits | INPUT ITERATIONS INSPECTION_INPUT]".into(),
        );
    }
    let iterations: usize = args[1].parse()?;
    if !(1..=64).contains(&iterations)
        || fs::metadata(&args[0])?.len() > (MAX_AGENT_COMPILER_REQUEST_BYTES + 1) as u64
        || fs::metadata(&args[2])?.len() > (MAX_AGENT_COMPILER_REQUEST_BYTES + 1) as u64
    {
        return Err("measurement exceeds its bounded fixture input or iteration limit".into());
    }
    let input = fs::read(&args[0])?;
    let mut timings = Vec::with_capacity(iterations);
    let mut response_digest = None;
    let mut response_bytes = 0;
    let mut outcome = String::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let response = compile_authoring_request_bytes(&input);
        timings.push(start.elapsed().as_nanos() as u64);
        let actual_digest = digest(&response);
        if response_digest
            .as_ref()
            .is_some_and(|expected| expected != &actual_digest)
        {
            return Err("native response changed for the same exact input".into());
        }
        response_digest = Some(actual_digest);
        response_bytes = response.len();
        let value: serde_json::Value = serde_json::from_slice(&response)?;
        outcome = value["outcome"]
            .as_str()
            .ok_or("missing response outcome")?
            .to_owned();
    }
    let inspection_input = fs::read(&args[2])?;
    let mut inspection_timings = Vec::with_capacity(iterations);
    let mut inspection_response_digest = None;
    let mut inspection_response_bytes = 0;
    let mut inspection_outcome = String::new();
    for _ in 0..iterations {
        let start = Instant::now();
        let response = inspect_source_request_bytes(&inspection_input);
        inspection_timings.push(start.elapsed().as_nanos() as u64);
        let actual_digest = digest(&response);
        if inspection_response_digest
            .as_ref()
            .is_some_and(|expected| expected != &actual_digest)
        {
            return Err("native inspection changed for the same exact source input".into());
        }
        inspection_response_digest = Some(actual_digest);
        inspection_response_bytes = response.len();
        let value: serde_json::Value = serde_json::from_slice(&response)?;
        inspection_outcome = value["outcome"]
            .as_str()
            .ok_or("missing inspection outcome")?
            .to_owned();
    }
    println!(
        "{}",
        json!({
        "schema_version": 1,
        "debug_assertions": cfg!(debug_assertions),
        "compiler_semantic_identity": compiler_semantic_identity(),
        "input_digest": digest(&input),
            "input_bytes": input.len(),
            "response_digest": response_digest,
            "response_bytes": response_bytes,
            "outcome": outcome,
            "compile_nanoseconds": timings,
            "source_inspection": {
                "input_digest": digest(&inspection_input),
                "input_bytes": inspection_input.len(),
                "response_digest": inspection_response_digest,
                "response_bytes": inspection_response_bytes,
                "outcome": inspection_outcome,
                "inspect_nanoseconds": inspection_timings,
            },
        })
    );
    Ok(())
}
