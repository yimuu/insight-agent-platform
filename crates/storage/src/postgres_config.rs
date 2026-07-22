//! SQLx-backed validation for PostgreSQL connection policy.

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgSslMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresHistoryUrlError {
    InvalidUrl,
    RemoteTlsRequired,
}

pub fn validate_postgres_history_url(database_url: &str) -> Result<(), PostgresHistoryUrlError> {
    if !raw_postgres_scheme_is_allowed(database_url) {
        return Err(PostgresHistoryUrlError::InvalidUrl);
    }

    let options = PgConnectOptions::from_str(database_url)
        .map_err(|_| PostgresHistoryUrlError::InvalidUrl)?;

    if matches!(options.get_ssl_mode(), PgSslMode::VerifyFull) {
        return Ok(());
    }
    if options.get_socket().is_some() || raw_postgres_host_is_exact_local(database_url) {
        return Ok(());
    }

    Err(PostgresHistoryUrlError::RemoteTlsRequired)
}

fn raw_postgres_scheme_is_allowed(database_url: &str) -> bool {
    database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")
}

fn raw_postgres_host_is_exact_local(database_url: &str) -> bool {
    last_query_value(database_url, &["host", "hostaddr"])
        .map(|host| exact_local_postgres_host(&host))
        .unwrap_or_else(|| {
            raw_authority_host(database_url)
                .as_deref()
                .map(exact_local_postgres_host)
                .unwrap_or(false)
        })
}

fn exact_local_postgres_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

fn last_query_value(database_url: &str, names: &[&str]) -> Option<String> {
    let query = raw_query(database_url)?;
    let mut value = None;

    for pair in query.split('&') {
        let (key, pair_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_query_component(key);
        if names.contains(&key.as_str()) {
            value = Some(decode_query_component(pair_value));
        }
    }

    value
}

fn decode_query_component(component: &str) -> String {
    let bytes = component.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                if let (Some(high), Some(low)) =
                    (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
                {
                    decoded.push((high << 4) | low);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn raw_query(database_url: &str) -> Option<&str> {
    let (_, rest) = database_url.split_once('?')?;
    Some(rest.split_once('#').map_or(rest, |(query, _)| query))
}

fn raw_authority_host(database_url: &str) -> Option<String> {
    let (_, rest) = database_url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        return Some(host_port[..=end].to_string());
    }
    Some(host_port.split(':').next().unwrap_or(host_port).to_string())
}
