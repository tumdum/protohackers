use anyhow::{bail, ensure, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

#[derive(Debug, PartialEq)]
pub struct TargetPopulation {
    pub species: String,
    pub min: u32,
    pub max: u32,
}

impl TargetPopulation {
    async fn decode(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let species = read_string(r).await?;
        let min = r.read_u32().await?;
        let max = r.read_u32().await?;
        Ok(Self { species, min, max })
    }

    async fn encode(&self, w: &mut (impl AsyncWriteExt + Unpin)) -> Result<()> {
        write_string(w, &self.species).await?;
        w.write_u32(self.min).await?;
        w.write_u32(self.max).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct ObservedPopulation {
    pub species: String,
    pub count: u32,
}

impl ObservedPopulation {
    async fn decode(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let species = read_string(r).await?;
        let count = r.read_u32().await?;
        Ok(Self { species, count })
    }

    async fn encode(&self, w: &mut (impl AsyncWriteExt + Unpin)) -> Result<()> {
        write_string(w, &self.species).await?;
        w.write_u32(self.count).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Action {
    Cull,
    Conserve,
}

impl Action {
    fn to_u8(&self) -> u8 {
        match self {
            Self::Cull => 0x90,
            Self::Conserve => 0xa0,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Message {
    Hello {
        protocol: String,
        version: u32,
    },
    Error {
        message: String,
    },
    Ok,
    DialAuthority {
        site: u32,
    },
    TargetPopulations {
        site: u32,
        populations: Vec<TargetPopulation>,
    },
    CreatePolicy {
        species: String,
        action: Action,
    },
    DeletePolicy {
        policy: u32,
    },
    PolicyResult {
        policy: u32,
    },
    SiteVisit {
        site: u32,
        populations: Vec<ObservedPopulation>,
    },
}

impl Message {
    fn id(&self) -> u8 {
        match self {
            Self::Hello { .. } => 0x50,
            Self::Error { .. } => 0x51,
            Self::Ok { .. } => 0x52,
            Self::DialAuthority { .. } => 0x53,
            Self::TargetPopulations { .. } => 0x54,
            Self::CreatePolicy { .. } => 0x55,
            Self::DeletePolicy { .. } => 0x56,
            Self::PolicyResult { .. } => 0x57,
            Self::SiteVisit { .. } => 0x58,
        }
    }

    pub async fn decode(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let id = r.read_u8().await?;
        let msg_len = r.read_u32().await?;
        ensure!(msg_len < 1024 * 1024);
        // Space for the rest of the message and checksum ignoring header
        let mut inner_buf = vec![0; (msg_len - 1 - 4) as usize];
        r.read_exact(&mut inner_buf).await?;
        let msg = match id {
            0x50 => Self::decode_hello(&mut inner_buf.as_slice()).await?,
            0x51 => Self::decode_error(&mut inner_buf.as_slice()).await?,
            0x52 => Self::decode_ok(&mut inner_buf.as_slice()).await?,
            0x53 => Self::decode_dialauthority(&mut inner_buf.as_slice()).await?,
            0x54 => Self::decode_targetpopulations(&mut inner_buf.as_slice()).await?,
            0x55 => Self::decode_createpolicy(&mut inner_buf.as_slice()).await?,
            0x56 => Self::decode_deletepolicy(&mut inner_buf.as_slice()).await?,
            0x57 => Self::decode_policyresult(&mut inner_buf.as_slice()).await?,
            0x58 => Self::decode_sitevisit(&mut inner_buf.as_slice()).await?,
            id => unimplemented!("msg with {id:0x} is not implemented"),
        };

        let mut buf = vec![];
        msg.encode(&mut buf).await?;
        ensure!(
            inner_buf.last().copied() == buf.last().copied(),
            "invalid cksum, expected {:?}, got {:?}",
            buf.last(),
            inner_buf.last(),
        );

        Ok(msg)
    }

    pub async fn encode(&self, w: &mut (impl AsyncWriteExt + Unpin)) -> Result<()> {
        let mut inner_buf = vec![];
        self.encode_inner(&mut inner_buf).await?;

        let len = (1 + 4 + inner_buf.len() + 1) as u32;
        let mut buf = vec![];
        buf.write_u8(self.id()).await?;
        buf.write_u32(len).await?;
        buf.write(&inner_buf).await?;
        let cksum = (256 - buf.iter().fold(0u8, |a, b| a.overflowing_add(*b).0) as u16) as u8;
        buf.write_u8(cksum).await?;
        w.write(&buf).await?;
        Ok(())
    }

    async fn decode_hello(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let protocol = read_string(r).await?;
        // ensure!(protocol == "pestcontrol", "invalid protocol: '{protocol}'");
        let version = r.read_u32().await?;
        Ok(Message::Hello { protocol, version })
    }

    async fn decode_error(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let message = read_string(r).await?;
        Ok(Message::Error { message })
    }

    async fn decode_ok(_r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        Ok(Message::Ok {})
    }

    async fn decode_dialauthority(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let site = r.read_u32().await?;
        Ok(Message::DialAuthority { site })
    }

    async fn decode_targetpopulations(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let site = r.read_u32().await?;
        let populations_len = r.read_u32().await?;
        let mut populations = vec![];
        for _ in 0..populations_len {
            populations.push(TargetPopulation::decode(r).await?);
        }
        Ok(Message::TargetPopulations { site, populations })
    }

    async fn decode_createpolicy(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let species = read_string(r).await?;
        let action = match r.read_u8().await? {
            0x90 => Action::Cull,
            0xa0 => Action::Conserve,
            other => bail!("unexpected action {other:0x}"),
        };
        Ok(Message::CreatePolicy { species, action })
    }

    async fn decode_deletepolicy(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let policy = r.read_u32().await?;
        Ok(Message::DeletePolicy { policy })
    }

    async fn decode_policyresult(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let policy = r.read_u32().await?;
        Ok(Message::PolicyResult { policy })
    }

    async fn decode_sitevisit(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<Self> {
        let site = r.read_u32().await?;
        let populations_len = r.read_u32().await?;

        let mut populations = vec![];
        for _ in 0..populations_len {
            populations.push(ObservedPopulation::decode(r).await?);
        }
        Ok(Message::SiteVisit { site, populations })
    }

    async fn encode_inner(&self, w: &mut (impl AsyncWriteExt + Unpin)) -> Result<()> {
        match self {
            Self::Hello { protocol, version } => {
                write_string(w, protocol).await?;
                w.write_u32(*version).await?;
            }
            Self::Error { message } => {
                write_string(w, message).await?;
            }
            Self::Ok => {}
            Self::DialAuthority { site } => {
                w.write_u32(*site).await?;
            }
            Self::TargetPopulations { site, populations } => {
                w.write_u32(*site).await?;
                w.write_u32(populations.len() as u32).await?;
                for pop in populations {
                    pop.encode(w).await?;
                }
            }
            Self::CreatePolicy { species, action } => {
                write_string(w, species).await?;
                w.write_u8(action.to_u8()).await?;
            }
            Self::DeletePolicy { policy } => {
                w.write_u32(*policy).await?;
            }
            Self::PolicyResult { policy } => {
                w.write_u32(*policy).await?;
            }
            Self::SiteVisit { site, populations } => {
                w.write_u32(*site).await?;
                w.write_u32(populations.len() as u32).await?;
                for pop in populations {
                    pop.encode(w).await?;
                }
            }
        }
        Ok(())
    }
}

async fn read_string(r: &mut (impl AsyncBufReadExt + Unpin)) -> Result<String> {
    let len = r.read_u32().await?;
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await?;
    Ok(std::str::from_utf8(&buf)?.to_owned())
}

async fn write_string(w: &mut (impl AsyncWriteExt + Unpin), s: &str) -> Result<()> {
    w.write_u32(s.as_bytes().len() as u32).await?;
    w.write(s.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_hello() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x50, 0, 0, 0, 0x19, 0, 0, 0, 0xb, 0x70, 0x65, 0x73, 0x74, 0x63, 0x6f, 0x6e, 0x74,
            0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xce,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::Hello {
            protocol: "pestcontrol".to_owned(),
            version: 1,
        };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_hello_bad_checksum() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x50, 0, 0, 0, 0x19, 0, 0, 0, 0xb, 0x70, 0x65, 0x73, 0x74, 0x63, 0x6f, 0x6e, 0x74,
            0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, /* bad cksum */ 0x00,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        assert!(Message::decode(&mut input).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_hello_4_unused() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x50, 0x00, 0x00, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0xca,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        assert!(Message::decode(&mut input).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_hello_too_short() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x50, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x0b, 0x70, 0x65, 0x73, 0x74, 0x63,
            0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x00, 0x00, 0x00, 0x01, 0xca,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        assert!(Message::decode(&mut input).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_error() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x51, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x64, 0x78,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::Error {
            message: "bad".to_owned(),
        };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_ok() -> Result<()> {
        let input_bytes: &[u8] = &[0x52, 0x00, 0x00, 0x00, 0x06, 0xa8];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::Ok {};
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_dialauthority() -> Result<()> {
        let input_bytes: &[u8] = &[0x53, 0x0, 0x0, 0x0, 0xa, 0x00, 0x00, 0x30, 0x39, 0x3a];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::DialAuthority { site: 12345 };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_targetpopulations() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x54, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x03, 0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x0a, 0x80,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::TargetPopulations {
            site: 12345,
            populations: vec![
                TargetPopulation {
                    species: "dog".to_owned(),
                    min: 1,
                    max: 3,
                },
                TargetPopulation {
                    species: "rat".to_owned(),
                    min: 0,
                    max: 10,
                },
            ],
        };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_createpolicy() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x55, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0xa0, 0xc0,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::CreatePolicy {
            species: "dog".to_owned(),
            action: Action::Conserve,
        };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_deletepolicy() -> Result<()> {
        let input_bytes: &[u8] = &[0x56, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x7b, 0x25];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::DeletePolicy { policy: 123 };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_policyresult() -> Result<()> {
        let input_bytes: &[u8] = &[0x57, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x7b, 0x24];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::PolicyResult { policy: 123 };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }

    #[tokio::test]
    async fn test_sitevisit() -> Result<()> {
        let input_bytes: &[u8] = &[
            0x58, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x03, 0x64, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03,
            0x72, 0x61, 0x74, 0x00, 0x00, 0x00, 0x05, 0x8c,
        ];
        let mut input = BufReader::new(input_bytes.clone());
        let msg = Message::decode(&mut input).await?;
        let expected = Message::SiteVisit {
            site: 12345,
            populations: vec![
                ObservedPopulation {
                    species: "dog".to_owned(),
                    count: 1,
                },
                ObservedPopulation {
                    species: "rat".to_owned(),
                    count: 5,
                },
            ],
        };
        assert_eq!(expected, msg);
        let mut output = vec![];
        msg.encode(&mut output).await?;
        assert_eq!(input_bytes, output);
        Ok(())
    }
}
