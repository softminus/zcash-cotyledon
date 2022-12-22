pub mod dns;
pub mod grpc;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use zebra_chain::parameters::Network;

use crate::probe::classify::{get_classification, PeerStats, ProbeConfiguration};
use crate::probe::PeerClassification;
#[derive(Debug, Clone, Default)]
pub struct ServingNodes {
    pub primaries: HashSet<SocketAddr>,
    pub alternates: HashSet<SocketAddr>,
}

pub fn update_serving_nodes(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &HashMap<SocketAddr, Option<PeerStats>>,
    probes_config: &ProbeConfiguration,
) {
    let mut primary_nodes = HashSet::new();
    let mut alternate_nodes = HashSet::new();

    for (key, value) in internal_peer_tracker {
        if key.ip().is_global() {
            let classification = get_classification(&value, &key, Network::Mainnet, probes_config);
            if classification == PeerClassification::AllGood {
                primary_nodes.insert(key.clone());
            }
            if classification == PeerClassification::MerelySyncedEnough {
                alternate_nodes.insert(key.clone());
            }
        }
    }
    println!("primary nodes: {:?}", primary_nodes);
    println!("alternate nodes: {:?}", alternate_nodes);
    let new_nodes = ServingNodes {
        primaries: primary_nodes,
        alternates: alternate_nodes,
    };

    let mut unlocked = serving_nodes_shared.write().unwrap();
    *unlocked = new_nodes.clone();
}

pub fn single_node_update(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    new_peer_address: &SocketAddr,
    new_peer_stat: &Option<PeerStats>,
    probes_config: &ProbeConfiguration,
) {
    if new_peer_address.ip().is_global() {
        let old_nodes = serving_nodes_shared.read().unwrap();
        let mut primary_nodes = old_nodes.primaries.clone();
        let mut alternate_nodes = old_nodes.alternates.clone();
        drop(old_nodes);

        match get_classification(
            new_peer_stat,
            new_peer_address,
            Network::Mainnet,
            probes_config,
        ) {
            PeerClassification::AllGood => {
                primary_nodes.insert(new_peer_address.clone());
                alternate_nodes.remove(new_peer_address);
            }
            PeerClassification::MerelySyncedEnough => {
                primary_nodes.remove(new_peer_address);
                alternate_nodes.insert(new_peer_address.clone());
            }
            _ => {
                primary_nodes.remove(new_peer_address);
                alternate_nodes.remove(new_peer_address);
            }
        }

        let new_nodes = ServingNodes {
            primaries: primary_nodes,
            alternates: alternate_nodes,
        };

        let mut unlocked = serving_nodes_shared.write().unwrap();
        *unlocked = new_nodes.clone();
    }
}
