use crate::resp::RedisValueRef;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    PING,
    ECHO(String),
    Set(String, String),
    Get(String),
}

#[derive(Debug, thiserror::Error)]
pub enum CmdParseError {
    #[error("Incomplete command: {0}")]
    IncompleteCommand(String),
    #[error("Expected string, got: {0:?}")]
    NotString(RedisValueRef),
}

pub fn parse_cmds(raw_cmds: Vec<RedisValueRef>) -> Result<Vec<Cmd>, CmdParseError> {
    let mut res = Vec::with_capacity(raw_cmds.len());

    let mut iter = raw_cmds.into_iter().peekable();

    while let Some(raw_cmd) = iter.next() {
        let cmd_str = match raw_cmd.as_str() {
            Some(s) => s,
            None => return Err(CmdParseError::NotString(raw_cmd)),
        };

        // resp cmds are case-insensitive
        let cmd_str = cmd_str.to_lowercase();
        let cmd = match cmd_str.as_str() {
            "ping" => Cmd::PING,
            "echo" => {
                let msg = next_string_arg_or_error(iter.next(), &cmd_str)?;
                Cmd::ECHO(msg)
            }
            "set" => {
                let key = next_string_arg_or_error(iter.next(), &cmd_str)?;
                let val = next_string_arg_or_error(iter.next(), &cmd_str)?;
                Cmd::Set(key, val)
            }
            "get" => {
                let key = next_string_arg_or_error(iter.next(), &cmd_str)?;
                Cmd::Get(key)
            }
            _ => {
                unreachable!("unknown command: {:?}", raw_cmd);
            }
        };

        res.push(cmd);
    }

    Ok(res)
}

fn next_string_arg_or_error(
    o: Option<RedisValueRef>,
    cmd_str: &str,
) -> Result<String, CmdParseError> {
    match o.and_then(|v| v.to_string()) {
        Some(s) => Ok(s),
        None => Err(CmdParseError::IncompleteCommand(cmd_str.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_echo_and_ping_cmds() {
        let input = vec![
            RedisValueRef::String("PING".into()),
            RedisValueRef::String("ECHO".into()),
            RedisValueRef::String("HELLO".into()),
        ];
        let cmds = parse_cmds(input).unwrap();
        assert_eq!(cmds, vec![Cmd::PING, Cmd::ECHO("HELLO".to_string())]);
    }

    #[test]
    fn test_set_and_get_cmds() {
        let input = vec![
            RedisValueRef::String("SET".into()),
            RedisValueRef::String("key".into()),
            RedisValueRef::String("value".into()),
            RedisValueRef::String("GET".into()),
            RedisValueRef::String("key".into()),
        ];

        let cmds = parse_cmds(input).unwrap();
        assert_eq!(
            cmds,
            vec![
                Cmd::Set("key".to_string(), "value".to_string()),
                Cmd::Get("key".to_string()),
            ]
        );
    }
}
