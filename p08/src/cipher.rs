use anyhow::{bail, ensure, Result};

#[derive(Debug, Clone)]
pub struct Cipher {
    ops: Vec<Op>,
}

#[derive(Debug, Clone, Copy)]
enum Op {
    ReverseBits,
    Xor(u8),
    XorPos,
    Add(u8),
    AddPos,
}

use Op::*;

impl Cipher {
    pub fn new(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() > 1, "empty spec is invalid");
        ensure!(
            bytes.last() == Some(&0),
            "need to have 0 as last op in cipher, got {bytes:?}"
        );

        let mut ops = vec![];
        let mut i = 0;
        while i < bytes.len() - 1 {
            let op = match bytes[i] {
                1 => ReverseBits,
                2 => Xor(bytes[i + 1]),
                3 => XorPos,
                4 => Add(bytes[i + 1]),
                5 => AddPos,
                n => bail!("unsupported op code {n}"),
            };
            ops.push(op);

            if matches!(op, Xor(_)) || matches!(op, Add(_)) {
                i += 2;
            } else {
                i += 1;
            }
        }

        Ok(Self { ops })
    }

    pub fn encode_one(&self, start_offset: usize, input: u8) -> Result<u8> {
        let mut b = input;
        for op in &self.ops {
            match op {
                ReverseBits => {
                    b = b.reverse_bits();
                }
                Add(n) => {
                    b = ((b as usize + *n as usize) % 256) as u8;
                }
                AddPos => {
                    b = ((b as usize + start_offset) % 256) as u8;
                }
                Xor(n) => {
                    b = b ^ n;
                }
                XorPos => {
                    b = b ^ ((start_offset) % 256) as u8;
                }
            }
        }
        Ok(b)
    }

    pub fn encode(&self, start_offset: usize, input: &[u8]) -> Result<Vec<u8>> {
        let out: Result<Vec<_>, _> = input
            .iter()
            .enumerate()
            .map(|(i, b)| self.encode_one(start_offset + i, *b))
            .collect();
        let out = out?;
        ensure!(input != out, "no change to input");
        Ok(out)
    }

    pub fn decode_one(&self, start_offset: usize, input: u8) -> Result<u8> {
        let mut b = input;
        for op in self.ops.iter().rev() {
            match op {
                ReverseBits => {
                    b = b.reverse_bits();
                }
                Add(n) => {
                    b = ((b as i64 - *n as i64) % 256) as u8;
                }
                AddPos => {
                    b = ((b as i64 - start_offset as i64) % 256) as u8;
                }
                Xor(n) => {
                    b = b ^ n;
                }
                XorPos => {
                    b = b ^ ((start_offset) % 256) as u8;
                }
            }
        }
        Ok(b)
    }

    pub fn decode(&self, start_offset: usize, input: &[u8]) -> Result<Vec<u8>> {
        let out: Result<Vec<_>, _> = input
            .iter()
            .enumerate()
            .map(|(i, b)| self.decode_one(start_offset + i, *b))
            .collect();
        let out = out?;
        ensure!(input != out, "no change to input");
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_ciphers() -> Result<()> {
        let cipher = Cipher::new(&[2, 1, 1, 0])?;
        let encoded = cipher.encode(0, b"hello").unwrap();
        assert_eq!(encoded, [0x96, 0x26, 0xb6, 0xb6, 0x76]);
        assert_eq!(b"hello".to_vec(), cipher.decode(0, &encoded)?);

        let cipher = Cipher::new(&[5, 5, 0])?;
        let encoded = cipher.encode(0, b"hello").unwrap();
        assert_eq!(encoded, [0x68, 0x67, 0x70, 0x72, 0x77]);
        assert_eq!(b"hello".to_vec(), cipher.decode(0, &encoded)?);

        Ok(())
    }

    #[test]
    fn roundtrip() -> Result<()> {
        let cipher = Cipher::new(&[1, 2, 230, 3, 4, 240, 5, 0])?;
        for off in 0..1000 {
            let encoded = cipher.encode(off, b"hello")?;
            assert_eq!(b"hello".to_vec(), cipher.decode(off, &encoded)?);
        }

        Ok(())
    }

    #[test]
    fn decode() -> Result<()> {
        let cipher = Cipher::new(&[0x02, 0x7b, 0x05, 0x01, 0x00])?;
        let encoded = cipher.encode(0, b"4x dog,5x car\n")?;
        assert_eq!(
            encoded,
            [0xf2, 0x20, 0xba, 0x44, 0x18, 0x84, 0xba, 0xaa, 0xd0, 0x26, 0x44, 0xa4, 0xa8, 0x7e]
        );
        assert_eq!(b"4x dog,5x car\n".to_vec(), cipher.decode(0, &encoded)?);

        let encoded = cipher.encode(0, b"5x car\n")?;
        assert_eq!(encoded, [0x72, 0x20, 0xba, 0xd8, 0x78, 0x70, 0xee]);
        assert_eq!(b"5x car\n".to_vec(), cipher.decode(0, &encoded)?);

        let encoded = cipher.encode(14, b"3x rat,2x cat\n")?;
        assert_eq!(
            encoded,
            [0x6a, 0x48, 0xd6, 0x58, 0x34, 0x44, 0xd6, 0x7a, 0x98, 0x4e, 0x0c, 0xcc, 0x94, 0x31]
        );
        assert_eq!(b"3x rat,2x cat\n".to_vec(), cipher.decode(14, &encoded)?);

        let encoded = cipher.encode(7, b"3x rat\n")?;
        assert_eq!(encoded, [0xf2, 0xd0, 0x26, 0xc8, 0xa4, 0xd8, 0x7e]);
        assert_eq!(b"3x rat\n".to_vec(), cipher.decode(7, &encoded)?);

        Ok(())
    }

    #[test]
    fn noop_ciphers() -> Result<()> {
        assert!(Cipher::new(&[0]).is_err());

        let cipher = Cipher::new(&[2, 0, 0])?;
        assert!(dbg!(cipher.encode(0, b"hello")).is_err());

        let cipher = Cipher::new(&[2, 0xab, 2, 0xab, 0])?;
        assert!(dbg!(cipher.encode(0, b"hello")).is_err());

        let cipher = Cipher::new(&[1, 1, 0])?;
        assert!(dbg!(cipher.encode(0, b"hello")).is_err());

        let cipher = Cipher::new(&[0x02, 0xa0, 0x02, 0x0b, 0x02, 0xab, 0x00])?;
        assert!(dbg!(cipher.encode(0, b"hello")).is_err());
        Ok(())
    }
}
