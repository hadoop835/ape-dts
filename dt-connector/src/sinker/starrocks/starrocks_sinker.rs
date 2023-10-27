use std::time::{SystemTime, UNIX_EPOCH};

use crate::Sinker;

use dt_common::error::Error;

use dt_meta::{ddl_data::DdlData, row_data::RowData};

use reqwest::{
    header::{AUTHORIZATION, EXPECT},
    Client, Method,
};

use async_trait::async_trait;
use base64::encode;

#[derive(Clone)]
pub struct StarRocksSinker {
    pub url: String,
    pub batch_size: usize,
    pub client: Client,
}

#[async_trait]
impl Sinker for StarRocksSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        // https://docs.starrocks.io/en-us/latest/loading/Load_to_Primary_Key_tables#implementation
        //
        // let test_data = r#"[{"id":10086,"name":"test","comment":"stream load2","op": 1}]"#;
        // op(__op): 0 -> upset(default), 1 -> delete
        //
        // Mock Table Struct:
        //
        //   CREATE TABLE `dts_test` (
        //     `id` int ,
        //     `name` string ,
        //     `comment` string
        //   ) PRIMARY KEY (id)
        //   DISTRIBUTED BY HASH(id)
        let test_data = r#"10086,test,stream load2,1"#;
        self.send_data(test_data).await.unwrap();

        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn refresh_meta(&mut self, _data: Vec<DdlData>) -> Result<(), Error> {
        Ok(())
    }
}

impl StarRocksSinker {
    async fn send_data(&self, content: &str) -> Result<(), Error> {
        // Todo:
        // starrocks default port: 8030->FE http, 8040->BE http
        // the authentication information may be lost during the http redirect from fe to be, resulting in an error:
        //    'no valid Basic authorization'
        let load_url = format!("http://{}/_stream_load", self.url);

        // let client_builder = ClientBuilder::new()
        //     .redirect(reqwest::redirect::Policy::custom(|_attempt| true));

        let label_tmp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let entity = content.to_owned();

        // todo: sinker dimension reuse
        let put = self
            .client
            .request(Method::PUT, &load_url)
            .header(EXPECT, "100-continue")
            .header(AUTHORIZATION, self.basic_auth_header("root", "").unwrap())
            .header("label", label_tmp) // each stream loader batch use an exact label
            .header("format", "csv") // csv;json, default is csv
            // .header("strip_outer_array", "true") // set when format=json
            .header("columns", "id, name, test, op, __op = op")
            .header("column_separator", ",")
            .body(entity);

        let put_request = put.build().unwrap();

        let response = self.client.execute(put_request).await.unwrap();
        let status = response.status();
        let load_result = &response.text().await.unwrap();

        println!("status: {}, load_result: {}", status.as_str(), load_result);
        Ok(())
    }

    fn basic_auth_header(&self, username: &str, password: &str) -> Result<String, Error> {
        let tobe_encode = format!("{}:{}", username, password);
        let encoded = encode(tobe_encode);
        let auth_header = format!("Basic {}", encoded);
        Ok(auth_header)
    }
}
