use reqwest::Url;

pub(crate) fn validate_image_url(value: &str) -> Result<(), ()> {
    if value.trim().is_empty() {
        return Err(());
    }
    let parsed = Url::parse(value).map_err(|_| ())?;
    match parsed.scheme() {
        "http" | "https" if parsed.host_str().is_some() => Ok(()),
        "data" => validate_image_data_url(value),
        _ => Err(()),
    }
}

fn validate_image_data_url(value: &str) -> Result<(), ()> {
    let prefix = value.get(..5).ok_or(())?;
    if !prefix.eq_ignore_ascii_case("data:") {
        return Err(());
    }
    let (metadata, payload) = value.get(5..).ok_or(())?.split_once(',').ok_or(())?;
    if payload.trim().is_empty() {
        return Err(());
    }

    let mut metadata_parts = metadata.split(';');
    let media_type = metadata_parts.next().ok_or(())?;
    let (top_level, subtype) = media_type.split_once('/').ok_or(())?;
    if !top_level.eq_ignore_ascii_case("image")
        || !is_mime_token(top_level)
        || !is_mime_token(subtype)
    {
        return Err(());
    }

    let mut saw_base64 = false;
    for parameter in metadata_parts {
        if parameter.eq_ignore_ascii_case("base64") {
            if saw_base64 {
                return Err(());
            }
            saw_base64 = true;
            continue;
        }
        if saw_base64 {
            return Err(());
        }
        let (name, value) = parameter.split_once('=').ok_or(())?;
        if !is_mime_token(name) || !is_data_parameter_value(value) {
            return Err(());
        }
    }
    if saw_base64 {
        validate_base64_payload(payload)
    } else {
        validate_percent_encoded_payload(payload)
    }
}

fn validate_base64_payload(payload: &str) -> Result<(), ()> {
    if payload.is_empty() || !payload.is_ascii() {
        return Err(());
    }
    let padding = payload
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count();
    if padding > 2 {
        return Err(());
    }
    let data_len = payload.len().checked_sub(padding).ok_or(())?;
    if !payload.as_bytes()[..data_len]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
    {
        return Err(());
    }
    match padding {
        0 if data_len % 4 != 1 => Ok(()),
        1 if payload.len().is_multiple_of(4) && data_len % 4 == 3 => Ok(()),
        2 if payload.len().is_multiple_of(4) && data_len % 4 == 2 => Ok(()),
        _ => Err(()),
    }
}

fn validate_percent_encoded_payload(payload: &str) -> Result<(), ()> {
    if payload.is_empty() || !payload.is_ascii() {
        return Err(());
    }
    let bytes = payload.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if bytes
                .get(index + 1..index + 3)
                .is_none_or(|escape| !escape.iter().all(u8::is_ascii_hexdigit))
            {
                return Err(());
            }
            index += 3;
        } else if bytes[index].is_ascii_graphic() && bytes[index] != b'#' {
            index += 1;
        } else {
            return Err(());
        }
    }
    Ok(())
}

fn is_mime_token(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(is_mime_token_byte)
}

fn is_data_parameter_value(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if bytes
                .get(index + 1..index + 3)
                .is_none_or(|escape| !escape.iter().all(u8::is_ascii_hexdigit))
            {
                return false;
            }
            index += 3;
        } else if is_mime_token_byte(bytes[index]) {
            index += 1;
        } else {
            return false;
        }
    }
    true
}

fn is_mime_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}
