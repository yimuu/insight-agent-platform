use insight_cli::{execute_to_writer, parse_command, SystemDoctorProbe};
use std::{env, io, process::ExitCode};

fn main() -> ExitCode {
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    let command = match parse_command(&arguments) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(error.exit_code());
        }
    };
    let current_directory = match env::current_dir() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("cannot determine the current directory: {error}");
            return ExitCode::FAILURE;
        }
    };
    let probe = SystemDoctorProbe;
    let result = {
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        execute_to_writer(command, &current_directory, &probe, &mut writer)
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if let Some(output) = error.output() {
                print!("{output}");
            }
            eprintln!("{error}");
            ExitCode::from(error.exit_code())
        }
    }
}
