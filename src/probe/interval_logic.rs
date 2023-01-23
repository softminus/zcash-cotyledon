
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};

use std::sync::{Arc, RwLock};

use futures_util::StreamExt;

use rlimit::{getrlimit, increase_nofile_limit, Resource};

use tokio::net::{TcpListener, UdpSocket};

use tokio::time::sleep;
use std::time::{Duration, SystemTime};
use tonic::transport::Server;

use zebra_chain::parameters::Network;

use crate::probe::Timeouts;

use crate::serving::dns::DnsContext;
use crate::serving::grpc::grpc_protocol::seeder_server::SeederServer;
use crate::serving::grpc::SeedContext;
use crate::serving::ServingNodes;

use crate::probe::classify::{get_classification, PeerStats, ProbeConfiguration};
use crate::probe::PeerClassification;
use crate::probe::ProbeType;

fn probe_schedule_for_node(
    peer_stats: &Option<PeerStats>,
    peer_address: &SocketAddr,
    network: Network,
    probes_config: &ProbeConfiguration,
) -> Vec<ProbeType> {
    let peer_classification = get_classification(peer_stats, peer_address, network, probes_config);
    match peer_classification {
        PeerClassification::Unknown => {
            println!(
                "node {:?} is Unknown, we try it this time around",
                peer_address
            );           
            return vec![ProbeType::Block]; // never tried a connection, so let's give it a try now
        }
        PeerClassification::BeyondUseless => {
            let threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 8), // base duration: 8 minutes
                16,                          // number of attempts in exponential warmup
                Duration::from_secs(60 * 60 * 16), // final duration: 16 hours
            );

            println!(
                "node {:?} is BeyondUseless, we try it again in {:?}",
                peer_address, threshold
            );
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold) {
               return vec![ProbeType::Block];
            } else {
                return vec![];
            }
        }
        PeerClassification::GenericBad => {
            let threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 4), // base duration: 4 minutes
                16,                          // number of attempts in exponential warmup
                Duration::from_secs(60 * 60 * 4), // final duration: 4 hours
            );
            println!(
                "node {:?} is GenericBad, we try it again in {:?}",
                peer_address, threshold
            );
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold) {
               return vec![ProbeType::Block];
            } else {
                return vec![];
            }
        }
        PeerClassification::EventuallyMaybeSynced => {
            let threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 2),  // base duration: 2 minutes
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 30), // final duration: 30 minutes
            );
            println!(
                "node {:?} is EventuallyMaybeSynced, we try it again in {:?}",
                peer_address, threshold
            );
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold) {
               return vec![ProbeType::Block];
            } else {
                return vec![];
            }
        }
        PeerClassification::MerelySyncedEnough => {
            let heavyweight_threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 4),  // base duration: 4 minutes
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 60), // final duration: 60 minutes
            );

            let lightweight_threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 1),  // base duration: 1 minute
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 15), // final duration: 15 minutes
            );

            println!(
                "node {:?} is MerelySyncedEnough, we try lightweight probe again in {:?}",
                peer_address, lightweight_threshold
            );

            println!(
                "node {:?} is MerelySyncedEnough, we try heavyweight probe again in {:?}",
                peer_address, heavyweight_threshold
            );

            let mut probes = Vec::new();
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), lightweight_threshold) {
                probes.push(ProbeType::Negotiation);
            }
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), heavyweight_threshold) {
                probes.push(ProbeType::Block);
            }

            if peer_stats.as_ref().unwrap().block_probe_valid == false {
                if !probes.contains(&ProbeType::Block) {
                    probes.push(ProbeType::Block);
                }
            }
            return probes;
        }
        PeerClassification::AllGood => {
            let heavyweight_threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 4),  // base duration: 4 minutes
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 60), // final duration: 60 minutes
            );

            let lightweight_threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 1),  // base duration: 1 minute
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 15), // final duration: 15 minutes
            );

            println!(
                "node {:?} is AllGood, we try lightweight probe again in {:?}",
                peer_address, lightweight_threshold
            );

            println!(
                "node {:?} is AllGood, we try heavyweight probe again in {:?}",
                peer_address, heavyweight_threshold
            );

            let mut probes = Vec::new();
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), lightweight_threshold) {
                probes.push(ProbeType::Negotiation);
            }
            if peer_last_polled_comparison(peer_stats.as_ref().unwrap(), heavyweight_threshold) {
                probes.push(ProbeType::Block);
            }
            if peer_stats.as_ref().unwrap().block_probe_valid == false {
                if !probes.contains(&ProbeType::Block) {
                    probes.push(ProbeType::Block);
                }
            }
            return probes;
        }
    }
}

fn peer_last_polled_comparison(peer_stats: &PeerStats, duration_threshold: Duration) -> bool {
    match peer_stats.protocol_negotiation.last_polled {
        None => true,
        Some(previous_polling_time) => {
            match SystemTime::now().duration_since(previous_polling_time) {
                Ok(duration) => return duration > duration_threshold,
                _ => true,
            }
        }
    }
}

fn exponential_acquisition_threshold_secs(
    peer_stats: &PeerStats,
    base_duration: Duration,
    attempts_cap: u64,
    final_duration: Duration,
) -> Duration {
    if peer_stats.protocol_negotiation.attempt_count < attempts_cap {
        Duration::from_secs(peer_stats.protocol_negotiation.attempt_count * base_duration.as_secs())
    } else {
        final_duration
    }
}
