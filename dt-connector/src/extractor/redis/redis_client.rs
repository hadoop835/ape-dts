use super::redis_resp_reader::RedisRespReader;
use super::redis_resp_types::Value;
use crate::sinker::redis::cmd_encoder::CmdEncoder;
use async_std::io::BufReader;
use async_std::net::TcpStream;
use async_std::prelude::*;
use dt_common::error::Error;
use dt_meta::redis::redis_object::RedisCmd;

use url::Url;

pub struct RedisClient {
    pub url: String,
    stream: BufReader<TcpStream>,
}

impl RedisClient {
    pub async fn new(url: &str) -> Result<Self, Error> {
        let url_info = Url::parse(url).unwrap();
        let host = url_info.host_str().unwrap();
        let port = url_info.port().unwrap();
        let username = url_info.username();
        let password = url_info.password();

        let stream = TcpStream::connect(format!("{}:{}", host, port))
            .await
            .unwrap();
        let mut me = Self {
            url: url.into(),
            stream: BufReader::new(stream),
        };

        if let Some(password) = password {
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("AUTH");
            if !username.is_empty() {
                cmd.add_str_arg(username);
            }
            cmd.add_str_arg(password);

            me.send(&cmd).await?;
            if let Ok(value) = me.read().await {
                if let Value::Okay = value {
                    return Ok(me);
                }
            }
            return Err(Error::Unexpected(format!("can't cnnect redis: {}", url)));
        }

        Ok(me)
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.stream.get_mut().shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }

    pub async fn send_packed(&mut self, packed_cmd: &[u8]) -> Result<(), Error> {
        self.stream.get_mut().write_all(packed_cmd).await?;
        Ok(())
    }

    pub async fn send(&mut self, cmd: &RedisCmd) -> Result<(), Error> {
        self.send_packed(&CmdEncoder::encode(cmd)).await
    }

    pub async fn read(&mut self) -> Result<Value, String> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        resp_reader.decode(&mut self.stream).await
    }

    pub async fn read_with_len(&mut self) -> Result<(Value, usize), String> {
        let mut resp_reader = RedisRespReader { read_len: 0 };
        let value = resp_reader.decode(&mut self.stream).await?;
        Ok((value, resp_reader.read_len))
    }

    pub async fn read_raw(&mut self, length: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; length];
        // if length is bigger than buffer size of BufReader, the buf will be filled by 0,
        // so here we must read from inner TcpStream instead of BufReader
        // let n = self.stream.read(&mut buf).await.unwrap();
        let mut read_count = 0;
        while read_count < length {
            // use async_std::net::TcpStream instead of tokio::net::TcpStream, tokio TcpStream may stuck
            // when trying to get big data by multiple read.
            read_count += self.stream.get_mut().read(&mut buf[read_count..]).await?;
        }
        Ok(buf)
    }
}
