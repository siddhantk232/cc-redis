use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, PartialEq)]
pub enum Cmd {
    PING,
    ECHO(String),
}

pub async fn parse<T: tokio::io::AsyncRead + std::marker::Unpin>(
    mut input: BufReader<T>,
) -> Result<Vec<Cmd>, std::io::Error> {
    let mut buf = Vec::with_capacity(512);
    let read = input.read_until(b'\n', &mut buf).await?;

    if read == 0 {
        // handle errors
        return Ok(vec![]);
    }

    // client starts with array of bulk strings:
    // *<number-of-elements>\r\n<element-1>...<element-n>
    assert_eq!(buf[0], b'*');

    let len = read - 2; // -2 to remove \r\n
    let elem_count = String::from_utf8(buf[1..len].to_vec()).unwrap();
    let mut elem_count: i64 = elem_count.parse().unwrap();

    let mut raw_cmds = Vec::with_capacity(elem_count as usize);

    while elem_count > 0 {
        buf.truncate(0);
        input.read_until(b'\n', &mut buf).await?;

        // bulk strings always start with $
        assert_eq!(buf[0], b'$');

        // length of bulk string can be ignored
        // let strlen = &buf[1..buf.len()];
        // // ignore. we can just read until \n
        // let strlen = i64::from_be_bytes(strlen.try_into().unwrap());
        // dbg!("received a bulk string of len: ", strlen);

        buf.truncate(0);
        let read = input.read_until(b'\n', &mut buf).await?;
        let len = read - 2; // -2 to remove \r\n
        let raw_cmd = String::from_utf8(buf[..len].to_vec()).unwrap();

        raw_cmds.push(raw_cmd);

        elem_count -= 1;
    }

    let mut cmds = Vec::with_capacity(raw_cmds.len());
    let mut raw_cmds = raw_cmds.into_iter();
    while let Some(raw_cmd) = raw_cmds.next() {
        // redis is case-insensitive
        let cmd = match raw_cmd.to_lowercase().as_str() {
            "ping" => Cmd::PING,
            "echo" => {
                let msg = raw_cmds.next().unwrap();
                Cmd::ECHO(msg)
            }
            _ => {
                unreachable!("unknown command: {}", raw_cmd);
            }
        };

        cmds.push(cmd);
    }

    Ok(cmds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse() {
        let input = b"*3\r\n$4\r\nPING\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
        let reader: tokio::io::BufReader<&[u8]> = BufReader::new(input);

        let cmds = parse(reader).await.unwrap();
        assert_eq!(cmds, vec![Cmd::PING, Cmd::ECHO("HELLO".to_string())]);
    }
}
