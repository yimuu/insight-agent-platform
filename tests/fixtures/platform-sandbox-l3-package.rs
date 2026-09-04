//! Linux-only Package fixture for the real OpenSandbox/Kubernetes L3 qualification.
//!
//! The immutable Armed runner invokes this binary directly. It deliberately has no Platform
//! dependencies or credentials and emits exactly one bounded JSON value on stdout.

use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Read as _, Write as _},
    net::{IpAddr, SocketAddr, TcpStream},
    os::raw::{c_int, c_uint},
    process::ExitCode,
    str::FromStr as _,
    thread,
    time::Duration,
};

unsafe extern "C" {
    fn close(file_descriptor: c_int) -> c_int;
    fn fork() -> c_int;
    fn getppid() -> c_int;
    fn kill(process_id: c_int, signal: c_int) -> c_int;
    fn setsid() -> c_int;
    fn setuid(user_id: u32) -> c_int;
    fn sleep(seconds: c_uint) -> c_uint;
    fn _exit(status: c_int) -> !;
}

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
        "boundary" => {
            let marker = arguments.next().ok_or("missing boundary marker")?;
            if !marker.starts_with("/tmp/insight-boundary-") || arguments.next().is_some() {
                return Err("boundary marker is invalid");
            }
            let status = fs::read_to_string("/proc/self/status")
                .map_err(|_| "failed to read process status")?;
            let effective_uid = process_status_field(&status, "Uid:")?
                .split_ascii_whitespace()
                .nth(1)
                .ok_or("effective uid is absent")?
                .parse::<u32>()
                .map_err(|_| "effective uid is invalid")?;
            let effective_capabilities = u64::from_str_radix(
                process_status_field(&status, "CapEff:")?.trim(),
                16,
            )
            .map_err(|_| "effective capabilities are invalid")?;
            let state_write_allowed = OpenOptions::new()
                .append(true)
                .open("/run/insight-sandbox/authority/activation.latch")
                .is_ok();
            // SAFETY: signal zero performs only a permission/existence check against this
            // Package's direct parent.
            let runner_signal_allowed = unsafe { kill(getppid(), 0) == 0 };
            // SAFETY: this deliberately tries to cross back to the fixed runner UID. A correct
            // capability drop makes the syscall fail without changing this process credential.
            let runner_setuid_allowed = unsafe { setuid(65_532) == 0 };

            // SAFETY: the fixture is single-threaded. The child closes inherited protocol file
            // descriptors, tests session escape, delays, then writes only the supplied /tmp marker.
            let child = unsafe { fork() };
            if child < 0 {
                return Err("boundary child fork failed");
            }
            if child == 0 {
                unsafe {
                    close(0);
                    close(1);
                    close(2);
                    let escaped_session = setsid() >= 0;
                    sleep(2);
                    if let Ok(mut file) = OpenOptions::new().create_new(true).write(true).open(marker)
                    {
                        let _ = writeln!(file, "escaped_session={escaped_session}");
                    }
                    _exit(0);
                }
            }
            print!(
                "{{\"effective_capabilities\":{effective_capabilities},\"effective_uid\":{effective_uid},\"runner_setuid_allowed\":{runner_setuid_allowed},\"runner_signal_allowed\":{runner_signal_allowed},\"state_write_allowed\":{state_write_allowed}}}"
            );
        }
        _ => return Err("unknown operation"),
    }
    Ok(())
}

fn process_status_field<'a>(status: &'a str, name: &str) -> Result<&'a str, &'static str> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .ok_or("process status field is absent")
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
