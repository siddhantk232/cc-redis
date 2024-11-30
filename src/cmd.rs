use crate::resp::RedisValueRef;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set(String, RedisValueRef, Option<Expiry>),
    Get(String),
    Incr(String),
    /// Start of a transaction
    Multi,
}

#[derive(Debug, PartialEq)]
pub enum Expiry {
    Ex(i64),
    Px(i64),
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
            "ping" => Cmd::Ping,
            "echo" => {
                let msg = next_string_arg(&mut iter, &cmd_str)?;
                Cmd::Echo(msg)
            }
            "set" => {
                let key = next_string_arg(&mut iter, &cmd_str)?;
                let val = next_arg(&mut iter, &cmd_str)?;

                // TODO:
                // https://redis.io/docs/latest/commands/set/
                // let _nx = try_parse_arg(&mut iter, "nx");
                // let _xx = try_parse_arg(&mut iter, "xx");
                // let _xx = try_parse_arg(&mut iter, "get");

                let px = try_parse_arg_value(&mut iter, "px")?
                    .and_then(|x| x.to_string_int())
                    .map(Expiry::Px);

                let ex = try_parse_arg_value(&mut iter, "ex")?
                    .and_then(|x| x.to_string_int())
                    .map(Expiry::Ex);

                Cmd::Set(key, val, px.or(ex))
            }
            "get" => {
                let key = next_string_arg(&mut iter, &cmd_str)?;
                Cmd::Get(key)
            }
            "incr" => {
                let key = next_string_arg(&mut iter, &cmd_str)?;
                Cmd::Incr(key)
            }
            "multi" => Cmd::Multi,
            _ => {
                unreachable!("unknown command: {:?}", raw_cmd);
            }
        };

        res.push(cmd);
    }

    Ok(res)
}

#[allow(unused)]
fn try_parse_arg(
    iter: &mut std::iter::Peekable<impl Iterator<Item = RedisValueRef>>,
    arg: &str,
) -> Option<RedisValueRef> {
    if let Some(val) = iter.peek() {
        match val.as_str() {
            Some(val) if val.to_lowercase() == arg => iter.next(),
            _ => None,
        }
    } else {
        None
    }
}

#[allow(unused)]
fn next_arg(
    iter: &mut impl Iterator<Item = RedisValueRef>,
    cmd_str: &str,
) -> Result<RedisValueRef, CmdParseError> {
    let o = iter.next();
    match o {
        Some(s) => Ok(s),
        None => Err(CmdParseError::IncompleteCommand(cmd_str.to_string())),
    }
}

/// Parse the value of the next argument if it's present
/// Example: SET foo bar [px 100 | ex 100]
/// This will parse the value of px or ex
fn try_parse_arg_value(
    iter: &mut std::iter::Peekable<impl Iterator<Item = RedisValueRef>>,
    arg: &str,
) -> Result<Option<RedisValueRef>, CmdParseError> {
    if let Some(val) = iter.peek() {
        match val.as_str() {
            Some(val) if val.to_lowercase() == arg => {
                iter.next().expect("arg has been peeked");
                if let Some(val) = iter.next() {
                    Ok(Some(val))
                } else {
                    Err(CmdParseError::IncompleteCommand(arg.to_string()))
                }
            }
            _ => Ok(None),
        }
    } else {
        Ok(None)
    }
}

fn next_string_arg(
    iter: &mut impl Iterator<Item = RedisValueRef>,
    cmd_str: &str,
) -> Result<String, CmdParseError> {
    let o = iter.next();
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
        assert_eq!(cmds, vec![Cmd::Ping, Cmd::Echo("HELLO".to_string())]);
    }

    #[test]
    fn test_set_and_get_cmds() {
        let input = vec![
            RedisValueRef::String("SET".into()),
            RedisValueRef::String("key".into()),
            RedisValueRef::String("value".into()),
            RedisValueRef::String("GET".into()),
            RedisValueRef::String("key".into()),
            RedisValueRef::String("SET".into()),
            RedisValueRef::String("foo".into()),
            RedisValueRef::String("bar".into()),
            RedisValueRef::String("px".into()),
            RedisValueRef::String("100".into()),
            RedisValueRef::String("SET".into()),
            RedisValueRef::String("key".into()),
            RedisValueRef::Int(1),
        ];

        let cmds = parse_cmds(input).unwrap();
        assert_eq!(
            cmds,
            vec![
                Cmd::Set("key".to_string(), RedisValueRef::String("value".into()), None),
                Cmd::Get("key".to_string()),
                Cmd::Set("foo".to_string(), RedisValueRef::String("bar".into()), Some(Expiry::Px(100))),
                Cmd::Set("key".to_string(), RedisValueRef::Int(1), None),
            ]
        );
    }

    #[test]
    fn test_incr_cmd() {
        let input = vec![
            RedisValueRef::String("INCR".into()),
            RedisValueRef::String("key".into()),
        ];

        let cmds = parse_cmds(input).unwrap();
        assert_eq!(cmds, vec![Cmd::Incr("key".to_string()),]);
    }

    #[test]
    fn test_multi_cmd() {
        let input = vec![RedisValueRef::String("MULTI".into())];
        let cmds = parse_cmds(input).unwrap();
        assert_eq!(cmds, vec![Cmd::Multi]);
    }
}
