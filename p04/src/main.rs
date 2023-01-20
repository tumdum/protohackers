use anyhow::Result;
use std::collections::HashMap;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:4567").await?;
    let mut state: HashMap<Vec<u8>, Vec<u8>> = Default::default();
    state.insert(b"version".to_vec(), b"0.42".to_vec());

    loop {
        let mut buf = vec![0u8; 1024];
        let (len, addr) = socket.recv_from(&mut buf).await?;
        let buf = &buf[..len];
        if let Some(i) = buf.iter().position(|v| *v == b'=') {
            let key = &buf[..i];
            if key == b"version" {
                continue;
            }
            let val = &buf[i + 1..];
            state.insert(key.to_vec(), val.to_vec());
        } else {
            let mut val = match state.get(buf) {
                Some(v) => v.clone(),
                None => vec![],
            };
            let mut reply = buf.to_vec();
            reply.push(b'=');
            reply.append(&mut val);
            socket.send_to(&reply, addr).await?;
        }
    }
}
