
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

// if we have a node with an empty/invalidated block_probe, make sure to schedule a block probe ASAP


// fn poll_this_time_around(
//     peer_stats: &Option<PeerStats>,
//     peer_address: &SocketAddr,
//     network: Network,
// ) -> bool {
//     let peer_classification = get_classification(peer_stats, peer_address, network);
//     match peer_classification {
//         PeerClassification::Unknown => {
//             println!(
//                 "node {:?} is Unknown, we try it this time around",
//                 peer_address
//             );
//             true // never tried a connection, so let's give it a try
//         }
//         PeerClassification::BeyondUseless => {
//             let threshold = exponential_acquisition_threshold_secs(
//                 peer_stats.as_ref().unwrap(),
//                 Duration::from_secs(60 * 8), // base duration: 8 minutes
//                 16,                          // number of attempts in exponential warmup
//                 Duration::from_secs(60 * 60 * 16), // final duration: 16 hours
//             );
//             println!(
//                 "node {:?} is BeyondUseless, we try it again in {:?}",
//                 peer_address, threshold
//             );
//             peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
//         }
//         PeerClassification::GenericBad => {
//             let threshold = exponential_acquisition_threshold_secs(
//                 peer_stats.as_ref().unwrap(),
//                 Duration::from_secs(60 * 4), // base duration: 4 minutes
//                 16,                          // number of attempts in exponential warmup
//                 Duration::from_secs(60 * 60 * 4), // final duration: 4 hours
//             );
//             println!(
//                 "node {:?} is GenericBad, we try it again in {:?}",
//                 peer_address, threshold
//             );
//             peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
//         }
//         PeerClassification::EventuallyMaybeSynced => {
//             let threshold = exponential_acquisition_threshold_secs(
//                 peer_stats.as_ref().unwrap(),
//                 Duration::from_secs(60 * 2),  // base duration: 2 minutes
//                 16,                           // number of attempts in exponential warmup
//                 Duration::from_secs(60 * 30), // final duration: 30 minutes
//             );
//             println!(
//                 "node {:?} is EventuallyMaybeSynced, we try it again in {:?}",
//                 peer_address, threshold
//             );
//             peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
//         }
//         PeerClassification::MerelySyncedEnough => {
//             let threshold = exponential_acquisition_threshold_secs(
//                 peer_stats.as_ref().unwrap(),
//                 Duration::from_secs(60 * 2),  // base duration: 2 minutes
//                 16,                           // number of attempts in exponential warmup
//                 Duration::from_secs(60 * 30), // final duration: 30 minutes
//             );
//             println!(
//                 "node {:?} is MerelySyncedEnough, we try it again in {:?}",
//                 peer_address, threshold
//             );
//             peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
//         }
//         PeerClassification::AllGood => {
//             let threshold = exponential_acquisition_threshold_secs(
//                 peer_stats.as_ref().unwrap(),
//                 Duration::from_secs(60 * 2),  // base duration: 2 minutes
//                 16,                           // number of attempts in exponential warmup
//                 Duration::from_secs(60 * 30), // final duration: 30 minutes
//             );
//             println!(
//                 "node {:?} is AllGood, we try it again in {:?}",
//                 peer_address, threshold
//             );
//             peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
//         }
//     }
// }

// fn peer_last_polled_comparison(peer_stats: &PeerStats, duration_threshold: Duration) -> bool {
//     match peer_stats.last_polled {
//         None => true,
//         Some(previous_polling_time) => {
//             match SystemTime::now().duration_since(previous_polling_time) {
//                 Ok(duration) => return duration > duration_threshold,
//                 _ => true,
//             }
//         }
//     }
// }

// fn exponential_acquisition_threshold_secs(
//     peer_stats: &PeerStats,
//     base_duration: Duration,
//     attempts_cap: u64,
//     final_duration: Duration,
// ) -> Duration {
//     if peer_stats.total_attempts < attempts_cap {
//         Duration::from_secs(peer_stats.total_attempts * base_duration.as_secs())
//     } else {
//         final_duration
//     }
// }
