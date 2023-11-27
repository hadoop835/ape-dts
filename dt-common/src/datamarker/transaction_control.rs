use core::fmt;
use std::collections::HashMap;

use crate::{
    config::datamarker_config::{DataMarkerConfig, DataMarkerSettingEnum},
    error::Error,
};
use regex::Regex;

const TOPOLOLOGY_KEY: &str = "topology";
const SOURCE_NODE_KEY: &str = "source";
const SINK_NODE_KEY: &str = "sink";

#[derive(Clone, Default, Debug)]
pub struct TopologyInfo {
    pub topology_key: String,
    pub source_node: String,
    pub sink_node: String,
}

impl TopologyInfo {
    pub fn is_empty(&self) -> bool {
        self.topology_key.is_empty() || self.source_node.is_empty() || self.sink_node.is_empty()
    }
}

impl fmt::Display for TopologyInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "topology_key:{},source_node:{},sink_node:{}",
            self.topology_key, self.source_node, self.sink_node
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct TransactionWorker {
    pub transaction_db: String,
    pub transaction_table: String,
    pub transaction_express: String,
    pub white_nodes: String,
    pub black_nodes: String,
}

impl TransactionWorker {
    pub fn from(datamarker_config: &DataMarkerConfig) -> Self {
        match &datamarker_config.setting {
            Some(setting) => match setting {
                DataMarkerSettingEnum::Transaction {
                    transaction_db,
                    transaction_table,
                    transaction_express,
                    white_nodes,
                    black_nodes,
                    ..
                } => Self {
                    transaction_db: transaction_db.clone(),
                    transaction_table: transaction_table.clone(),
                    transaction_express: transaction_express.clone(),
                    white_nodes: white_nodes.clone(),
                    black_nodes: black_nodes.clone(),
                },
            },
            None => Self::default(),
        }
    }

    pub fn is_validate(&self) -> bool {
        !self.transaction_db.is_empty()
            && !self.transaction_table.is_empty()
            && !self.transaction_express.is_empty()
            && (!self.white_nodes.is_empty() || !self.black_nodes.is_empty())
    }

    pub fn pick_infos(&self, db: &str, table: &str) -> Result<Option<TopologyInfo>, Error> {
        if db.is_empty()
            || table.is_empty()
            || self.transaction_express.is_empty()
            || db != self.transaction_db
        {
            // the database is not match the transaction_db will return immediately.
            return Ok(None);
        }

        let regex_result = Regex::new(&self.transaction_express);
        match regex_result {
            Ok(regex) => {
                if let Some(caps) = regex.captures(table) {
                    return Ok(Some(TopologyInfo {
                        topology_key: caps.name(TOPOLOLOGY_KEY).unwrap().as_str().to_string(),
                        source_node: caps.name(SOURCE_NODE_KEY).unwrap().as_str().to_string(),
                        sink_node: caps.name(SINK_NODE_KEY).unwrap().as_str().to_string(),
                    }));
                }
            }
            Err(e) => {
                return Err(Error::ConfigError(e.to_string()));
            }
        }
        Ok(None)
    }

    // result: <(is_transaction_event, is_filter, is_from_cache), Error>
    pub fn is_filter(
        &self,
        db: &str,
        table: &str,
        current_topology: TopologyInfo,
        cache: &mut HashMap<(String, String), bool>,
    ) -> Result<(bool, bool, bool), Error> {
        if cache.contains_key(&(db.to_string(), table.to_string())) {
            let is_filter = cache
                .get(&(db.to_string(), table.to_string()))
                .unwrap_or(&false)
                .to_owned();
            return Ok((true, is_filter, true));
        }

        let do_filter;

        let pick_result = self.pick_infos(db, table);
        match pick_result {
            Ok(pick_option) => match pick_option {
                Some(pick) => do_filter = self.is_filter_internal(Some(pick), current_topology),
                // when db.table is not a transaction table. ignore insert into cache
                None => {
                    return Ok((
                        false,
                        self.is_filter_internal(None, current_topology),
                        false,
                    ))
                }
            },
            Err(e) => return Err(e),
        }

        cache.insert((db.to_string(), table.to_string()), do_filter);

        Ok((true, do_filter, false))
    }

    pub fn is_filter_internal(
        &self,
        topology_option: Option<TopologyInfo>,
        current_topology: TopologyInfo,
    ) -> bool {
        let str_to_slice = |s: &str| -> Vec<String> {
            s.split(',')
                .map(|str| str.to_string())
                .collect::<Vec<String>>()
        };

        let (topology_key, source_node_key): (String, String) = match topology_option {
            Some(tp) => (tp.topology_key, tp.source_node),
            // 'None' means that this event comes from the business write of source
            None => (
                current_topology.topology_key.clone(),
                current_topology.source_node.clone(),
            ),
        };

        // whether to filter by default depends on whether the 'white_nodes' configuration is specified
        let mut do_filter = !self.white_nodes.is_empty();
        if !self.black_nodes.is_empty()
            && topology_key == current_topology.topology_key
            && str_to_slice(&self.black_nodes).contains(&source_node_key)
        {
            // the priority of 'black_nodes' is higher than 'white_nodes'
            return true;
        }
        if !self.white_nodes.is_empty()
            && topology_key == current_topology.topology_key
            && str_to_slice(&self.white_nodes).contains(&source_node_key)
        {
            do_filter = false
        }

        do_filter
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn build_datamarker_config(
        transaction_db: String,
        transaction_table: String,
        transaction_express: String,
        transaction_command: String,
        white_nodes: String,
        black_nodes: String,
    ) -> DataMarkerConfig {
        DataMarkerConfig {
            setting: Some(DataMarkerSettingEnum::Transaction {
                transaction_db,
                transaction_table,
                transaction_express,
                transaction_command,
                white_nodes,
                black_nodes,
            }),
        }
    }

    fn build_worker(
        transaction_db: String,
        transaction_table: String,
        transaction_express: String,
        transaction_command: String,
        white_nodes: String,
        black_nodes: String,
    ) -> TransactionWorker {
        let config = build_datamarker_config(
            transaction_db,
            transaction_table,
            transaction_express,
            transaction_command,
            white_nodes,
            black_nodes,
        );
        TransactionWorker::from(&config)
    }

    fn build_topology_info(
        curr_topology: &str,
        curr_source_node: &str,
        curr_sink_node: &str,
    ) -> TopologyInfo {
        TopologyInfo {
            topology_key: curr_topology.to_string(),
            source_node: curr_source_node.to_string(),
            sink_node: curr_sink_node.to_string(),
        }
    }

    #[test]
    fn is_filter_test() {
        // black list
        let mut transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from(""),
            String::from("node1"),
        );

        let curr_topology = build_topology_info("topo1", "node1", "node2");

        let mut cache: HashMap<(String, String), bool> = HashMap::new();
        let mut result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo1_node1_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && filter),
            Err(_) => assert!(false),
        }

        result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo1_node3_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && !filter),
            Err(_) => assert!(false),
        }

        result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo2_node3_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && !filter),
            Err(_) => assert!(false),
        }

        assert_eq!(cache.len(), 3);

        result =
            transaction_worker.is_filter("test", "test_table", curr_topology.clone(), &mut cache);
        match result {
            Ok((is_trans, filter, _)) => {
                assert!(
                    !is_trans
                        && filter
                        && !cache.contains_key(&("test".to_string(), "test_table".to_string()))
                )
            }
            Err(_) => assert!(false),
        }

        result =
            transaction_worker.is_filter("ape_dt", "test_table", curr_topology.clone(), &mut cache);
        match result {
            Ok((is_trans, filter, _)) => {
                assert!(
                    !is_trans
                        && filter
                        && !cache.contains_key(&("ape_dt".to_string(), "test_table".to_string()))
                )
            }
            Err(_) => assert!(false),
        }

        assert_eq!(cache.len(), 3);

        // white list
        transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from("node1"),
            String::from(""),
        );

        cache.clear();

        result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo1_node1_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && !filter),
            Err(_) => assert!(false),
        }

        result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo1_node3_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && filter),
            Err(_) => assert!(false),
        }

        result = transaction_worker.is_filter(
            "ape_dt",
            "ape_dt_topo2_node3_node2",
            curr_topology.clone(),
            &mut cache,
        );
        match result {
            Ok((is_trans, filter, _)) => assert!(is_trans && filter),
            Err(_) => assert!(false),
        }

        assert_eq!(cache.len(), 3);

        result =
            transaction_worker.is_filter("test", "test_table", curr_topology.clone(), &mut cache);
        match result {
            Ok((is_trans, filter, _)) => {
                assert!(
                    !is_trans
                        && !filter
                        && !cache.contains_key(&("test".to_string(), "test_table".to_string()))
                )
            }
            Err(_) => assert!(false),
        }

        result =
            transaction_worker.is_filter("ape_dt", "test_table", curr_topology.clone(), &mut cache);
        match result {
            Ok((is_trans, filter, _)) => {
                assert!(
                    !is_trans
                        && !filter
                        && !cache.contains_key(&("test".to_string(), "test_table".to_string()))
                )
            }
            Err(_) => assert!(false),
        }

        assert_eq!(cache.len(), 3);

        // white list with business events
        transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from("node3"),
            String::from(""),
        );

        result =
            transaction_worker.is_filter("test", "test_table", curr_topology.clone(), &mut cache);
        match result {
            Ok((is_trans, filter, _)) => {
                assert!(
                    !is_trans
                        && filter
                        && !cache.contains_key(&("test".to_string(), "test_table".to_string()))
                )
            }
            Err(_) => assert!(false),
        }
    }

    #[test]
    fn pick_info_test() {
        let transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from(r"ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from(""),
            String::from("node1"),
        );

        let mut info_result = transaction_worker.pick_infos("ape_dt", "ape_dt_topo1_node1_node2");
        match info_result {
            Ok(info_option) => {
                let info = info_option.unwrap_or_default();
                assert_eq!(info.topology_key, "topo1");
                assert_eq!(info.source_node, "node1");
                assert_eq!(info.sink_node, "node2");
            }
            Err(_) => assert!(false),
        }

        info_result = transaction_worker.pick_infos("ape_dt", "ape_dt_wrongname");
        assert!(info_result.is_ok_and(|i| i.is_none()));
    }

    #[test]
    fn is_filter_internal_test() {
        let mut transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from(""),
            String::from("node1"),
        );

        let curr_topology = build_topology_info("topo1", "node1", "node2");

        // blacklist
        let mut is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo1"),
                source_node: String::from("node1"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(is_filter);

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo1"),
                source_node: String::from("node3"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(!is_filter);

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo2"),
                source_node: String::from("node1"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(!is_filter);

        // white list
        transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from("node1"),
            String::from(""),
        );

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo1"),
                source_node: String::from("node1"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(!is_filter);

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo1"),
                source_node: String::from("node3"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(is_filter);

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo2"),
                source_node: String::from("node3"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(is_filter);

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo2"),
                source_node: String::from("node1"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );

        assert!(is_filter);

        // black and white list
        transaction_worker = build_worker(
            String::from("ape_dt"),
            String::from("ape_dt_topo1_node1_node2"),
            String::from("ape_dt_(?P<topology>.*)_(?P<source>.*)_(?P<sink>.*)"),
            String::from("update ape_dt_topo1_node1_node2 set n = n + 1"),
            String::from("node1"),
            String::from("node1"),
        );

        is_filter = transaction_worker.is_filter_internal(
            Some(TopologyInfo {
                topology_key: String::from("topo1"),
                source_node: String::from("node1"),
                sink_node: String::from("node2"),
            }),
            curr_topology.clone(),
        );
        assert!(is_filter);
    }
}
