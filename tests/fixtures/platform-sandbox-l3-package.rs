//! Linux-only Package fixture for the real OpenSandbox/Kubernetes L3 qualification.
//!
//! The immutable Armed runner invokes this binary directly. It deliberately has no Platform
//! dependencies or credentials and emits exactly one bounded JSON value on stdout.

use std::{
    env,
    io::{self, Read as _},
    net::{IpAddr, SocketAddr, TcpStream},
    process::ExitCode,
    str::FromStr as _,
    thread,
    time::Duration,
};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), &'static str> {
    let mut arguments = env::args().skip(1);
    let operation = arguments.next().ok_or("missing operation")?;
    let mut input = Vec::new();
    io::stdin()
        .take(1_048_577)
        .read_to_end(&mut input)
        .map_err(|_| "failed to read input")?;
    if input.is_empty() || input.len() > 1_048_576 {
        return Err("input is empty or too large");
    }

    match operation.as_str() {
        "echo" => {
            if arguments.next().is_some() {
                return Err("echo has unexpected arguments");
            }
            print!("{}", String::from_utf8(input).map_err(|_| "input is not UTF-8")?);
        }
        "sleep-echo" => {
            let milliseconds = parse_u64(arguments.next(), "missing sleep duration")?;
            if milliseconds > 120_000 || arguments.next().is_some() {
                return Err("sleep duration is invalid");
            }
            thread::sleep(Duration::from_millis(milliseconds));
            print!("{}", String::from_utf8(input).map_err(|_| "input is not UTF-8")?);
        }
        "probe" => {
            let address = arguments.next().ok_or("missing probe address")?;
            let ip = IpAddr::from_str(&address).map_err(|_| "probe address is not an IP")?;
            let port = parse_u16(arguments.next(), "missing probe port")?;
            if port == 0 || arguments.next().is_some() {
                return Err("probe port is invalid");
            }
            let reachable = TcpStream::connect_timeout(
                &SocketAddr::new(ip, port),
                Duration::from_millis(1_000),
            )
            .is_ok();
            print!("{{\"network_reachable\":{reachable}}}");
        }
        _ => return Err("unknown operation"),
    }
    Ok(())
}

fn parse_u64(value: Option<String>, missing: &'static str) -> Result<u64, &'static str> {
    value
        .ok_or(missing)?
        .parse()
        .map_err(|_| "integer argument is invalid")
}

fn parse_u16(value: Option<String>, missing: &'static str) -> Result<u16, &'static str> {
    value
        .ok_or(missing)?
        .parse()
        .map_err(|_| "integer argument is invalid")
}
