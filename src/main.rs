#![feature(ip)]
#![feature(io_error_uncategorized)]
#![feature(io_error_more)]
#![feature(once_cell)]
use futures::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::Rng;
use rlimit::{getrlimit, increase_nofile_limit, Resource};
use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedReply, SeedRequest};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tonic::transport::Server;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use tower::Service;
use trust_dns_server::authority::MessageResponseBuilder;
use trust_dns_server::client::rr as dnsrr;
use trust_dns_server::proto::op as dnsop;
use trust_dns_server::server as dns;
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_chain::serialization::SerializationError;
use zebra_consensus::CheckpointList;
use zebra_network::types::{MetaAddr, PeerServices};
use zebra_network::{
    connect_isolated_tcp_direct, HandshakeError, InventoryResponse, Request, Response, Version,
};

enum CrawlingMode {
    FastAcquisition,
    LongTermUpdates,
}

struct Timeouts {
    hash_timeout: Duration,
    peers_timeout: Duration,
}

#[tokio::main]
async fn main() {
    if let Ok((_softlimit, hardlimit)) = getrlimit(Resource::NOFILE) {
        _ = increase_nofile_limit(hardlimit);
    }
    let max_inflight_conn = match getrlimit(Resource::NOFILE) {
        Ok((softlimit, _hardlimit)) => softlimit.min(1024), // limit it to avoid hurting NAT middleboxes
        _ => 128, // if we can't figure it out, play it really safe
    };

    let network = Network::Mainnet;
    let serving_nodes: ServingNodes = Default::default();
    let serving_nodes_shared = Arc::new(RwLock::new(serving_nodes));

    let seedfeed = SeedContext {
        serving_nodes_shared: serving_nodes_shared.clone(),
    };
    let addr = "127.0.0.1:50051".parse().unwrap(); // make me configurable
    let seeder_service = Server::builder()
        .add_service(SeederServer::new(seedfeed))
        .serve(addr);
    tokio::spawn(seeder_service);

    let dns_handler = DnsContext {
        serving_nodes_shared: serving_nodes_shared.clone(),
        serving_network: Network::Mainnet,
    };
    let mut dns_server = trust_dns_server::ServerFuture::new(dns_handler);

    // make us configurable
    dns_server.register_listener(
        TcpListener::bind("127.0.0.1:5301").await.unwrap(),
        Duration::from_secs(8),
    );
    dns_server.register_socket(UdpSocket::bind("127.0.0.1:5301").await.unwrap());
    tokio::spawn(dns_server.block_until_done());

    let initial_peer_addrs = [
        // make me configurable
        "20.47.97.70:8233",
        "51.79.229.21:8233",
        "34.73.242.102:8233",
        "162.19.139.183:8235",
        "51.210.208.202:8836",
        "221.223.25.99:2331",
        "85.15.179.171:8233",
        "195.201.111.115:8233",
        "94.156.174.100:8233",
        "8.209.65.101:8233",
        "23.88.71.118:8233",
        "51.77.64.51:8233",
        "8.210.14.154:8233",
        "124.126.140.196:2331",
        "47.242.8.170:8233",
        "8.214.158.97:8233",
        "85.214.219.243:8233",
        "54.238.23.140:8233",
        "51.81.184.90:30834",
        "116.202.53.174:8533",
        "88.80.148.28:8233",
        "203.96.179.202:8233",
        "84.75.28.247:8233",
        "39.97.172.77:8233",
        "51.79.57.29:8233",
        "51.77.64.59:8233",
        "178.128.101.61:8233",
        "51.77.64.61:8233",
        "194.135.81.61:8233",
        "46.4.50.226:8233",
        "54.145.30.137:8233",
        "64.201.122.142:54324",
        "34.202.104.227:30570",
        "39.97.233.19:8233",
        "142.132.212.130:8836",
        "104.233.147.162:8233",
        "51.81.154.19:30834",
        "51.178.76.73:8836",
        "144.217.11.155:8233",
        "13.231.190.41:8233",
        "18.235.47.206:8233",
        "51.222.254.36:8233",
        "65.108.41.222:20005",
        "54.84.155.205:8233",
        "46.249.236.211:8233",
        "162.19.139.183:8233",
        "173.212.197.63:8233",
        "176.34.40.41:8233",
        "5.196.80.197:8233",
        "91.206.16.214:8233",
        "79.43.86.64:8233",
        "157.90.88.178:9058",
        "50.7.29.20:8233",
        "23.129.64.30:8233",
        "162.55.103.190:8233",
        "47.254.69.198:8233",
        "116.62.229.19:8233",
        "51.210.220.135:8836",
        "51.81.184.89:30834",
        "51.79.229.21:8233",
        "104.207.139.34:8233",
        "37.59.32.10:8233",
        "47.90.209.31:8233",
        "51.210.216.77:8836",
        "3.252.40.246:5001",
        "18.217.102.40:8233",
        "15.235.85.30:8233",
        "139.99.123.157:8233",
        "159.89.26.105:8233",
        "65.108.41.222:21005",
        "51.195.62.151:8233",
        "51.79.230.103:8233",
        "8.210.73.119:8233",
        "157.245.172.190:8233",
        "3.72.134.66:8233",
        "51.210.208.201:8836",
        "162.19.139.181:8233",
        "141.95.45.187:30834",
        "161.97.155.203:8233",
        "35.91.16.78:8233",
        "51.178.76.85:8836",
        "73.172.228.152:8233",
        "142.132.202.124:8836",
        "52.28.203.21:8233",
        "120.24.79.67:8233",
        "20.47.97.70:8233",
        "65.108.220.35:8233",
        "47.89.158.145:8233",
        "47.75.194.174:8233",
        "18.189.228.115:8233",
        "116.203.188.195:8233",
        "157.90.89.105:9058",
        "116.202.170.226:8233",
        "8.218.11.43:8233",
        "51.210.216.76:8836",
        "51.195.234.88:2838",
        "178.234.34.18:8233",
        "165.232.125.107:8233",
        "136.243.145.143:8233",
        "135.181.18.180:8233",
        "95.217.78.170:8233",
        "88.198.48.91:8233",
        "97.119.97.142:8233",
        "39.97.242.143:8233",
        "123.114.100.178:2331",
        "8.219.76.216:8233",
        "5.2.75.10:8233",
        "91.199.137.99:8233",
        "5.9.74.158:8233",
        "35.233.224.178:8233",
        "78.46.46.252:8233",
        "34.255.6.39:5001",
        "162.19.139.182:8233",
        "3.16.30.39:8233",
        "47.242.184.215:8233",
        "51.210.114.183:8836",
        "47.252.44.174:8233",
        "35.230.70.77:8233",
        "135.181.79.230:8233",
        "65.21.137.242:8233",
        "47.254.176.240:8233",
        "37.187.88.208:8233",
        "168.119.147.118:8233",
        "47.243.196.68:8233",
        "162.19.136.65:30834",
        "34.196.173.50:8233",
        "8.218.10.114:8233",
        "62.210.69.194:8233",
        "209.141.47.197:8233",
        "44.197.66.202:8233",
        "35.72.109.227:8233",
        "44.200.177.58:8233",
        "51.222.245.186:8233",
        "111.90.145.162:8233",
        "135.148.55.16:8233",
        "51.195.63.10:30834",
        "65.21.40.28:8233",
        "46.4.192.189:8233",
        "78.189.206.225:8233",
        "8.209.80.185:8233",
        "147.135.11.134:8233",
    ];

    let mut internal_peer_tracker: HashMap<SocketAddr, Option<PeerStats>> = HashMap::new();

    for peer in initial_peer_addrs {
        let key = peer.to_socket_addrs().unwrap().next().unwrap();
        let value = None;
        internal_peer_tracker.insert(key, value);
    }
    let mut mode = CrawlingMode::FastAcquisition;

    loop {
        println!("starting Loop");
        match mode {
            CrawlingMode::FastAcquisition => {
                let timeouts = Timeouts {
                    peers_timeout: Duration::from_secs(8), // make me configurable
                    hash_timeout: Duration::from_secs(8),  // make me configurable
                };
                fast_walker(
                    &serving_nodes_shared,
                    &mut internal_peer_tracker,
                    network,
                    max_inflight_conn.try_into().unwrap(),
                    timeouts,
                    16, // make me configurable
                )
                .await;
                {
                    let serving_nodes_testing = serving_nodes_shared.read().unwrap();
                    if serving_nodes_testing.primaries.len()
                        + serving_nodes_testing.alternates.len()
                        > 3
                    // make me configurable
                    {
                        println!(
                            "SWITCHING TO SLOW WALKER, we are serving a total of {:?} nodes",
                            serving_nodes_testing.primaries.len()
                                + serving_nodes_testing.alternates.len()
                        );
                        mode = CrawlingMode::LongTermUpdates;
                    }
                }
                println!("FAST WALKER iteration done");
                sleep(Duration::new(8, 0)).await;
            }
            CrawlingMode::LongTermUpdates => {
                let timeouts = Timeouts {
                    peers_timeout: Duration::from_secs(32), // make me configurable
                    hash_timeout: Duration::from_secs(32),  // make me configurable
                };
                slow_walker(
                    &serving_nodes_shared,
                    &mut internal_peer_tracker,
                    network,
                    max_inflight_conn.try_into().unwrap(),
                    timeouts,
                    128, // make me configurable
                )
                .await;
            }
        }
        // just in case...we could add code to check if this does anything to find bugs with the incremental update
        update_serving_nodes(&serving_nodes_shared, &internal_peer_tracker);
    }
}

fn poll_this_time_around(
    peer_stats: &Option<PeerStats>,
    peer_address: &SocketAddr,
    network: Network,
) -> bool {
    let peer_classification = get_classification(peer_stats, peer_address, network);
    match peer_classification {
        PeerClassification::Unknown => {
            println!(
                "node {:?} is Unknown, we try it this time around",
                peer_address
            );
            true // never tried a connection, so let's give it a try
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
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
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
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
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
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
        PeerClassification::MerelySyncedEnough => {
            let threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 2),  // base duration: 2 minutes
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 30), // final duration: 30 minutes
            );
            println!(
                "node {:?} is MerelySyncedEnough, we try it again in {:?}",
                peer_address, threshold
            );
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
        PeerClassification::AllGood => {
            let threshold = exponential_acquisition_threshold_secs(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(60 * 2),  // base duration: 2 minutes
                16,                           // number of attempts in exponential warmup
                Duration::from_secs(60 * 30), // final duration: 30 minutes
            );
            println!(
                "node {:?} is AllGood, we try it again in {:?}",
                peer_address, threshold
            );
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
    }
}

fn peer_last_polled_comparison(peer_stats: &PeerStats, duration_threshold: Duration) -> bool {
    match peer_stats.last_polled {
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
    if peer_stats.total_attempts < attempts_cap {
        Duration::from_secs(peer_stats.total_attempts * base_duration.as_secs())
    } else {
        final_duration
    }
}

async fn slow_walker(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &mut HashMap<SocketAddr, Option<PeerStats>>,
    network: Network,
    max_inflight_conn: usize,
    timeouts: Timeouts,
    smear_cap: u64,
) {
    let mut rng = rand::thread_rng();
    let mut batch_queries = Vec::new();
    let doryphore = Arc::new(Semaphore::new(max_inflight_conn));
    for (proband_address, peer_stat) in internal_peer_tracker.iter() {
        if poll_this_time_around(peer_stat, proband_address, network) {
            batch_queries.push(Box::pin(hash_probe_and_update(
                proband_address.clone(),
                peer_stat.clone(),
                network,
                &timeouts,
                Duration::from_secs(rng.gen_range(0..smear_cap)),
                doryphore.clone(),
            ))
                as Pin<Box<dyn Future<Output = (SocketAddr, ProbeResult)>>>);
            batch_queries.push(Box::pin(probe_for_peers_two(
                proband_address.clone(),
                network,
                &timeouts,
                Duration::from_secs(rng.gen_range(0..smear_cap)),
                doryphore.clone(),
            )))
        } else {
            println!(
                "NOT POLLING {:?} THIS TIME AROUND, WE POLLED TOO RECENTLY",
                proband_address
            );
        }
    }

    let mut stream = futures::stream::iter(batch_queries).buffer_unordered(max_inflight_conn);
    while let Some(probe_result) = stream.next().await {
        let peer_address = probe_result.0;

        match probe_result.1 {
            ProbeResult::HashResult(new_peer_stat) => {
                println!(
                    "{:?} has new peer stat, which classifies it as a {:?}: {:?}",
                    peer_address,
                    get_classification(&Some(new_peer_stat.clone()), &peer_address, network),
                    new_peer_stat
                );
                internal_peer_tracker.insert(peer_address.clone(), Some(new_peer_stat.clone()));
                single_node_update(
                    &serving_nodes_shared,
                    &peer_address,
                    &Some(new_peer_stat.clone()),
                );
                println!("HashMap len: {:?}", internal_peer_tracker.len());
            }
            ProbeResult::MustRetryHashResult => {
                println!("Slow Walker probing {:?} for hashes failed due to too many open sockets, this should NOT HAPPEN", peer_address);
            }
            ProbeResult::PeersResult(new_peers) => {
                for peer in new_peers {
                    let key = peer.addr().to_socket_addrs().unwrap().next().unwrap();
                    if !internal_peer_tracker.contains_key(&key) {
                        println!("Slow Walker Probing {:?} yielded new peer {:?}, adding to peer tracker", peer_address, key);
                        internal_peer_tracker.insert(key.clone(), <Option<PeerStats>>::None);
                    }
                }
            }
            ProbeResult::PeersFail => {
                println!(
                    "Slow Walker probing {:?} for peers failed, will be retried next time around",
                    peer_address
                );
            }
            ProbeResult::MustRetryPeersResult => {
                println!("Slow Walker probing {:?} for peers failed due to too many open sockets, this should NOT HAPPEN", peer_address);
            }
        }
    }
}

async fn fast_walker(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &mut HashMap<SocketAddr, Option<PeerStats>>,
    network: Network,
    max_inflight_conn: usize,
    timeouts: Timeouts,
    smear_cap: u64,
) {
    let mut handles = FuturesUnordered::new();
    let doryphore = Arc::new(Semaphore::new(max_inflight_conn));

    let mut rng = rand::thread_rng();
    for (proband_address, peer_stat) in internal_peer_tracker.iter() {
        handles.push(Box::pin(hash_probe_and_update(
            proband_address.clone(),
            peer_stat.clone(),
            network,
            &timeouts,
            Duration::from_secs(rng.gen_range(0..smear_cap)),
            doryphore.clone(),
        ))
            as Pin<Box<dyn Future<Output = (SocketAddr, ProbeResult)>>>);
        handles.push(Box::pin(probe_for_peers_two(
            proband_address.clone(),
            network,
            &timeouts,
            Duration::from_secs(rng.gen_range(0..smear_cap)),
            doryphore.clone(),
        )));
    }
    while let Some(probe_result) = handles.next().await {
        let peer_address = probe_result.0;
        match probe_result.1 {
            ProbeResult::HashResult(new_peer_stat) => {
                println!(
                    "{:?} has new peer stat, which classifies it as a {:?}: {:?}",
                    peer_address,
                    get_classification(&Some(new_peer_stat.clone()), &peer_address, network),
                    new_peer_stat
                );
                internal_peer_tracker.insert(peer_address.clone(), Some(new_peer_stat.clone()));
                single_node_update(
                    &serving_nodes_shared,
                    &peer_address,
                    &Some(new_peer_stat.clone()),
                );
                println!("HashMap len: {:?}", internal_peer_tracker.len());
            }
            ProbeResult::MustRetryHashResult => {
                // we gotta retry
                println!("Retrying hash probe for {:?}", peer_address);
                handles.push(Box::pin(hash_probe_and_update(
                    peer_address.clone(),
                    internal_peer_tracker[&peer_address].clone(),
                    network,
                    &timeouts,
                    Duration::from_secs(rng.gen_range(0..smear_cap)),
                    doryphore.clone(),
                )));
            }
            ProbeResult::PeersResult(new_peers) => {
                for peer in new_peers {
                    let key = peer.addr().to_socket_addrs().unwrap().next().unwrap();
                    if !internal_peer_tracker.contains_key(&key) {
                        println!(
                            "Probing {:?} yielded new peer {:?}, adding to work queue",
                            peer_address, key
                        );
                        internal_peer_tracker.insert(key.clone(), <Option<PeerStats>>::None);
                        handles.push(Box::pin(hash_probe_and_update(
                            key.clone(),
                            <Option<PeerStats>>::None,
                            network,
                            &timeouts,
                            Duration::from_secs(rng.gen_range(0..smear_cap)),
                            doryphore.clone(),
                        )));
                        handles.push(Box::pin(probe_for_peers_two(
                            key.clone(),
                            network,
                            &timeouts,
                            Duration::from_secs(rng.gen_range(0..smear_cap)),
                            doryphore.clone(),
                        )));
                    }
                }
            }
            ProbeResult::PeersFail => {
                println!("probing {:?} for peers failed, not retrying", peer_address);
            }
            ProbeResult::MustRetryPeersResult => {
                println!("Retrying peers probe for {:?}", peer_address);
                handles.push(Box::pin(probe_for_peers_two(
                    peer_address.clone(),
                    network,
                    &timeouts,
                    Duration::from_secs(rng.gen_range(0..smear_cap)),
                    doryphore.clone(),
                )));
            }
        }
    }
}
