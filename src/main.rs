#![feature(ip)]
#![feature(io_error_uncategorized)]
#![feature(once_cell)]
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::{Arc, LazyLock, RwLock};
use tokio::time::sleep;
use std::time::{Duration, SystemTime};
use tower::Service;
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_network::types::PeerServices;
use zebra_network::{connect_isolated_tcp_direct, InventoryResponse, Request, Response, Version};
//use zebra_network::protocol::external::types::Version;
use rand::Rng;
use rlimit::{getrlimit, increase_nofile_limit, Resource};
use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedReply, SeedRequest};
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use trust_dns_server::authority::MessageResponseBuilder;
use trust_dns_server::client::rr as dnsrr;
use trust_dns_server::proto::op as dnsop;
use trust_dns_server::server as dns;
use zebra_consensus::CheckpointList;
use zebra_network::types::MetaAddr;
pub mod seeder_proto {
    tonic::include_proto!("seeder"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct SeedContext {
    serving_nodes_shared: Arc<RwLock<ServingNodes>>,
}

#[tonic::async_trait]
impl Seeder for SeedContext {
    async fn seed(
        &self,
        request: TonicRequest<SeedRequest>, // Accept request of type SeedRequest
    ) -> Result<TonicResponse<SeedReply>, Status> {
        // Return an instance of type SeedReply
        println!("Got a request: {:?}", request);
        let serving_nodes = self.serving_nodes_shared.read().unwrap();

        let mut primary_nodes_strings = Vec::new();
        let mut alternate_nodes_strings = Vec::new();

        for peer in serving_nodes.primaries.iter() {
            primary_nodes_strings.push(format!("{:?}", peer))
        }

        for peer in serving_nodes.alternates.iter() {
            alternate_nodes_strings.push(format!("{:?}", peer))
        }

        let reply = seeder_proto::SeedReply {
            primaries: primary_nodes_strings,
            alternates: alternate_nodes_strings,
        };

        Ok(TonicResponse::new(reply)) // Send back our formatted greeting
    }
}

#[derive(Clone, Debug)]
pub struct DnsContext {
    serving_nodes_shared: Arc<RwLock<ServingNodes>>,
    serving_network: Network,
}

#[async_trait::async_trait]
impl dns::RequestHandler for DnsContext {
    async fn handle_request<R: dns::ResponseHandler>(
        &self,
        request: &dns::Request,
        response_handle: R,
    ) -> dns::ResponseInfo {
        match self.do_handle_request(request, response_handle).await {
            Some(response_info) => response_info,
            None => {
                println!("Failed to respond to query: {:?}", request.query());
                let mut header = dnsop::Header::new();
                header.set_response_code(dnsop::ResponseCode::ServFail);
                header.into()
            }
        }
    }
}

impl DnsContext {
    async fn do_handle_request<R: dns::ResponseHandler>(
        &self,
        request: &dns::Request,
        mut response_handle: R,
    ) -> Option<dns::ResponseInfo> {
        if request.op_code() != dnsop::OpCode::Query {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(response_handle.send_response(response.error_msg(request.header(), dnsop::ResponseCode::ServFail)).await.unwrap());
        }
        if request.message_type() != dnsop::MessageType::Query {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(response_handle.send_response(response.error_msg(request.header(), dnsop::ResponseCode::ServFail)).await.unwrap());
        }
        if request.query().query_class() != dnsrr::DNSClass::IN {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(response_handle.send_response(response.error_msg(request.header(), dnsop::ResponseCode::ServFail)).await.unwrap());
        }
        let endpoint = dnsrr::LowerName::from(dnsrr::Name::from_str("dnsseed.z.cash").unwrap());
        if *request.query().name() != endpoint {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(response_handle.send_response(response.error_msg(request.header(), dnsop::ResponseCode::NXDomain)).await.unwrap());
        }

        let builder = MessageResponseBuilder::from_message_request(request);
        let mut header = dnsop::Header::response_from_request(request.header());
        header.set_authoritative(true);
        let mut records = Vec::new();
        {
            let serving_nodes = self.serving_nodes_shared.read().unwrap();
            for peer in serving_nodes
                .primaries
                .iter()
                .chain(serving_nodes.alternates.iter())
            {
                if dns_servable(*peer, self.serving_network) {
                    match request.query().query_type() {
                        dnsrr::RecordType::A => {
                            if let IpAddr::V4(ipv4) = peer.ip() {
                                records.push(dnsrr::Record::from_rdata(
                                    request.query().name().into(),
                                    60,
                                    dnsrr::RData::A(ipv4),
                                ))
                            }
                        },
                        dnsrr::RecordType::AAAA => {
                            if let IpAddr::V6(ipv6) = peer.ip() {
                                records.push(dnsrr::Record::from_rdata(
                                    request.query().name().into(),
                                    60,
                                    dnsrr::RData::AAAA(ipv6),
                                ))
                            }
                        }
                    _ => {} // if the query is something other than A or AAAA, we'll have no records in the reply, and that means a NODATA
                    }
                }
            }
        }

        let response = builder.build(header, records.iter(), &[], &[], &[]);
        Some(response_handle.send_response(response).await.unwrap())
    }
}
#[derive(Debug, Clone)]
enum PollStatus {
    RetryConnection(),                 // our fault
    ConnectionFail(),                  // their fault
    BlockRequestFail(PeerDerivedData), // their fault
    BlockRequestOK(PeerDerivedData),   // good
}
#[derive(Debug, Clone)]
struct PeerDerivedData {
    numeric_version: Version,
    peer_services: PeerServices,
    peer_height: Height,
    _user_agent: String,
    _relay: bool,
}

static HASH_CHECKPOINTS_MAINNET: LazyLock<HashSet<Hash>> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Mainnet);
    let mut proband_heights = HashSet::new();
    let mut proband_hashes = HashSet::new();
    for offset in (0..3200).rev() {
        if let Some(ht) =
            checkpoint.min_height_in_range((checkpoint.max_height() - offset).unwrap()..)
        {
            proband_heights.insert(ht);
        }
    }
    let mut proband_heights_vec = Vec::from_iter(proband_heights);
    proband_heights_vec.sort();
    for proband_height in proband_heights_vec.iter().rev().take(2) {
        if let Some(hash) = checkpoint.hash(*proband_height) {
            proband_hashes.insert(hash);
            println!(
                "preparing proband hashes...height {:?} has hash {:?}",
                proband_height, hash
            );
        }
    }
    proband_hashes
});

static REQUIRED_MAINNET_HEIGHT: LazyLock<Height> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Mainnet);
    checkpoint.max_height()
});

static REQUIRED_TESTNET_HEIGHT: LazyLock<Height> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Testnet);
    checkpoint.max_height()
});


static HASH_CHECKPOINTS_TESTNET: LazyLock<HashSet<Hash>> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Testnet);
    let mut proband_heights = HashSet::new();
    let mut proband_hashes = HashSet::new();
    for offset in (0..3200).rev() {
        if let Some(ht) =
            checkpoint.min_height_in_range((checkpoint.max_height() - offset).unwrap()..)
        {
            proband_heights.insert(ht);
        }
    }
    let mut proband_heights_vec = Vec::from_iter(proband_heights);
    proband_heights_vec.sort();
    for proband_height in proband_heights_vec.iter().rev().take(2) {
        if let Some(hash) = checkpoint.hash(*proband_height) {
            proband_hashes.insert(hash);
            println!(
                "preparing proband hashes...height {:?} has hash {:?}",
                proband_height, hash
            );
        }
    }
    proband_hashes
});


// , tcp_timeout: Duration, protocol_timeout: Duration

async fn test_a_server(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> PollStatus {
    println!("Starting new hash probe connection: peer addr is {:?}", peer_addr);
    let connection = connect_isolated_tcp_direct(
        network,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let connection = timeout(connection_timeout, connection);
    let connection = connection.await;

    let hash_checkpoints = match network {
        Network::Mainnet => HASH_CHECKPOINTS_MAINNET.clone(),
        Network::Testnet => HASH_CHECKPOINTS_TESTNET.clone()
    };

    match connection {
        Err(timeout_error) => {
            println!(
                "Probe connection with {:?} TIMED OUT: {:?}",
                peer_addr, timeout_error
            );
            PollStatus::ConnectionFail()
        }
        Ok(connection_might_have_failed) => {
            match connection_might_have_failed {
                Err(connection_failure) => {
                    println!(
                        "Connection with {:?} failed: {:?}",
                        peer_addr, connection_failure
                    );
                    if let Some(decanisterized_error) =
                        connection_failure.downcast_ref::<std::io::Error>()
                    {
                        println!(
                            "IO error detected... decannisterizing: error = {:?}, test={:?}",
                            decanisterized_error,
                            decanisterized_error.kind() == std::io::ErrorKind::Uncategorized
                        );
                        // Connection with XXX.XXX.XXX.XXX:8233 failed: Os { code: 24, kind: Uncategorized, message: "Too many open files" }
                        if decanisterized_error.kind() == std::io::ErrorKind::Uncategorized {
                            // probably an EMFILES / ENFILES
                            return PollStatus::RetryConnection();
                        }
                    }
                    PollStatus::ConnectionFail() // need to special-case this for ETOOMANYOPENFILES
                }
                Ok(mut good_connection) => {
                    let numeric_version = good_connection.connection_info.remote.version;
                    let peer_services = good_connection.connection_info.remote.services;
                    let peer_height = good_connection.connection_info.remote.start_height;
                    let _user_agent = good_connection.connection_info.remote.user_agent.clone();
                    let _relay = good_connection.connection_info.remote.relay;
                    let peer_derived_data = PeerDerivedData {
                        numeric_version,
                        peer_services,
                        peer_height,
                        _user_agent,
                        _relay,
                    };
                    // println!("remote peer version: {:?}", z.connection_info.remote.version >= Version(170_100));
                    // println!("remote peer services: {:?}", z.connection_info.remote.services.intersects(PeerServices::NODE_NETWORK));
                    // println!("remote peer height @ time of connection: {:?}", z.connection_info.remote.start_height >= Height(1_700_000));

                    let hash_query_response = good_connection
                        .call(Request::BlocksByHash(hash_checkpoints.clone()))
                        .await;
                    //println!("hash query response is {:?}", hash_query_response);
                    match hash_query_response {
                        Err(protocol_error) => {
                            println!("protocol failure after requesting blocks by hash with peer {}: {:?}", peer_addr, protocol_error);
                            return PollStatus::BlockRequestFail(peer_derived_data);
                        }
                        Ok(hash_query_protocol_response) => {
                            match hash_query_protocol_response {
                                Response::Blocks(block_vector) => {
                                    let mut returned_hashes = HashSet::new();
                                    for block_k in block_vector {
                                        if let InventoryResponse::Available(actual_block) = block_k
                                        {
                                            returned_hashes.insert(actual_block.hash());
                                        }
                                    }
                                    //println!("{:?}", returned_hashes);
                                    let intersection_count =
                                        returned_hashes.intersection(&hash_checkpoints).count();
                                    // println!("intersection_count is {:?}", intersection_count);
                                    if intersection_count == hash_checkpoints.len() {
                                        // All requested blocks are there and hash OK
                                        println!("Peer {:?} returned good hashes", peer_addr);
                                        return PollStatus::BlockRequestOK(peer_derived_data);
                                    }
                                    // node returned a Blocks response, but it wasn't complete/correct for some reason
                                    PollStatus::BlockRequestFail(peer_derived_data)
                                } // Response::Blocks(block_vector)
                                _ => {
                                    // connection established but we didn't get a Blocks response
                                    PollStatus::BlockRequestFail(peer_derived_data)
                                }
                            } // match hash_query_protocol_response
                        } // Ok(hash_query_protocol_response)
                    } // match hash_query_response
                } // Ok(good_connection)
            }
        }
    }
}

async fn probe_for_peers(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> Option<Vec<MetaAddr>> {
    println!("Starting peer probe connection: peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(
        network,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let the_connection = timeout(connection_timeout, the_connection);
    let x = the_connection.await;
    if let Ok(x) = x {
        match x {
            Ok(mut z) => {
                let mut peers_vec: Option<Vec<MetaAddr>> = None;
                for _attempt in 0..2 {
                    let resp = z.call(Request::Peers).await;
                    if let Ok(zebra_network::Response::Peers(ref candidate_peers)) = resp {
                        if candidate_peers.len() > 1 {
                            peers_vec = Some(candidate_peers.to_vec());
                            //println!("{:?}", peers_vec);
                            break;
                        }
                    }
                }
                return peers_vec;
            } // ok connect
            Err(error) => {
                println!("Peers connection with {:?} failed: {:?}", peer_addr, error);
                return None;
            }
        };
    } else {
        println!("Peers connection with {:?} TIMED OUT: {:?}", peer_addr, x);
        None
    }
}

//Connection with 74.208.91.217:8233 failed: Serialization(Parse("getblocks version did not match negotiation"))

#[derive(Debug, Clone, Copy, Default)]
struct EWMAState {
    scale: Duration,
    weight: f64,
    count: f64,
    reliability: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PeerClassification {
    AllGood, // Node meets all the legacy criteria (including uptime), and is fully servable to clients
    MerelySyncedEnough, // Node recently could serve us a recent-enough block (it is syncing or has synced to zcash chain)
    // but doesn't meet uptime criteria.
    Bad,           // Node is bad for some reason, but we could connect to it
    BeyondUseless, // We tried, but weren't even able to negotiate a connection
    Unknown,       // We got told about this node but haven't yet queried it
}

#[derive(Debug, Clone)]
struct PeerStats {
    total_attempts: u64,
    total_successes: u64,
    ewma_pack: EWMAPack,
    last_polled: Option<SystemTime>,
    last_success: Option<SystemTime>,

    peer_derived_data: Option<PeerDerivedData>,
}

#[derive(Debug, Clone, Default)]
struct ServingNodes {
    primaries: HashSet<SocketAddr>,
    alternates: HashSet<SocketAddr>,
}

#[derive(Debug, Clone, Copy)]
struct EWMAPack {
    stat_2_hours: EWMAState,
    stat_8_hours: EWMAState,
    stat_1day: EWMAState,
    stat_1week: EWMAState,
    stat_1month: EWMAState,
}

impl Default for EWMAPack {
    fn default() -> Self {
        EWMAPack {
            stat_2_hours: EWMAState {
                scale: Duration::new(3600 * 2, 0),
                ..Default::default()
            },
            stat_8_hours: EWMAState {
                scale: Duration::new(3600 * 8, 0),
                ..Default::default()
            },
            stat_1day: EWMAState {
                scale: Duration::new(3600 * 24, 0),
                ..Default::default()
            },
            stat_1week: EWMAState {
                scale: Duration::new(3600 * 24 * 7, 0),
                ..Default::default()
            },
            stat_1month: EWMAState {
                scale: Duration::new(3600 * 24 * 30, 0),
                ..Default::default()
            },
        }
    }
}
fn update_ewma(prev: &mut EWMAState, sample_age: Duration, sample: bool) {
    let weight_factor = (-sample_age.as_secs_f64() / prev.scale.as_secs_f64()).exp();
    // I don't understand what `count` and `weight` compute and why:
    prev.count = prev.count * weight_factor + 1.0;
    // `weight` only got used for `ignore` and `ban`, both features we left behind
    prev.weight = prev.weight * weight_factor + (1.0 - weight_factor);

    let sample_value: f64 = sample as i32 as f64;
    //println!("sample_value is: {}, weight_factor is {}", sample_value, weight_factor);
    prev.reliability = prev.reliability * weight_factor + sample_value * (1.0 - weight_factor);
}

fn update_ewma_pack(
    prev: &mut EWMAPack,
    previous_polling_time: Option<SystemTime>,
    current_polling_time: SystemTime,
    sample: bool,
) {
    let mut sample_age = Duration::from_secs(60 * 60 * 2); // default weighting, in case we haven't polled it yet

    if let Some(previous_polling_time) = previous_polling_time {
        if let Ok(duration) = current_polling_time.duration_since(previous_polling_time) {
            sample_age = duration
        }
    }
    update_ewma(&mut prev.stat_2_hours, sample_age, sample);
    update_ewma(&mut prev.stat_8_hours, sample_age, sample);
    update_ewma(&mut prev.stat_1day, sample_age, sample);
    update_ewma(&mut prev.stat_1week, sample_age, sample);
    update_ewma(&mut prev.stat_1month, sample_age, sample);
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
            println!(
                "node {:?} is BeyondUseless, we try it again in 8 hours",
                peer_address
            );
            peer_last_polled_comparison(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(8 * 60 * 60), // 8 hours, it's likely garbage
            )
        }
        PeerClassification::Bad => {
            println!("node {:?} is Bad, we try it again in 2 hours", peer_address);
            peer_last_polled_comparison(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(2 * 60 * 60), // 2 hours
            )
        }
        PeerClassification::MerelySyncedEnough => {
            println!(
                "node {:?} is MerelySyncedEnough, we try it again in {:?}",
                peer_address,
                Duration::from_secs(exponential_acquisition_threshold_secs(peer_stats.as_ref().unwrap())/2)
            );
            peer_last_polled_comparison(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(exponential_acquisition_threshold_secs(peer_stats.as_ref().unwrap())/2)
            )
        }
        PeerClassification::AllGood => {
            println!(
                "node {:?} is AllGood, we try it again in {:?}",
                peer_address,
                Duration::from_secs(exponential_acquisition_threshold_secs(peer_stats.as_ref().unwrap()))
            );
            peer_last_polled_comparison(
                peer_stats.as_ref().unwrap(),
                Duration::from_secs(exponential_acquisition_threshold_secs(peer_stats.as_ref().unwrap()))
            )
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

fn exponential_acquisition_threshold_secs(peer_stats: &PeerStats) -> u64 {
    if peer_stats.total_attempts < 32 {
        peer_stats.total_attempts * 2 * (60 as u64)
    } else {
        32 * 60
    }
}

fn get_classification(
    peer_stats: &Option<PeerStats>,
    peer_address: &SocketAddr,
    network: Network,
) -> PeerClassification {
    let peer_stats = match peer_stats {
        None => return PeerClassification::Unknown,
        Some(peer_stats) => peer_stats,
    };

    if peer_stats.total_attempts == 0 {
        // we never pinged them
        return PeerClassification::Unknown;
    }

    if peer_stats.peer_derived_data.is_none() {
        // we tried, but never been able to negotiate a connection
        return PeerClassification::BeyondUseless;
    }

    let peer_derived_data = peer_stats.peer_derived_data.as_ref().unwrap();

    if !peer_derived_data
        .peer_services
        .intersects(PeerServices::NODE_NETWORK)
    {
        return PeerClassification::Bad;
    }

    if peer_derived_data.numeric_version < required_serving_version(network) {
        return PeerClassification::Bad;
    }

    if peer_derived_data.peer_height < required_height(network) {
        return PeerClassification::Bad;
    }

    if !peer_address.ip().is_global() {
        return PeerClassification::Bad;
    }
    let ewmas = peer_stats.ewma_pack;
    if peer_stats.total_attempts <= 3 && peer_stats.total_successes * 2 >= peer_stats.total_attempts
    {
        return PeerClassification::AllGood;
    };

    if ewmas.stat_2_hours.reliability > 0.85 && ewmas.stat_2_hours.count > 2.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_8_hours.reliability > 0.70 && ewmas.stat_8_hours.count > 4.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_1day.reliability > 0.55 && ewmas.stat_1day.count > 8.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_1week.reliability > 0.45 && ewmas.stat_1week.count > 16.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_1month.reliability > 0.35 && ewmas.stat_1month.count > 32.0 {
        return PeerClassification::AllGood;
    };

    // at least one success in the last 2 hours
    if let Some(last_success) = peer_stats.last_success {
        if let Ok(duration) = last_success.elapsed() {
            if duration <= Duration::from_secs(60 * 60 * 2) {
                return PeerClassification::MerelySyncedEnough;
            }
        }
    }
    return PeerClassification::Unknown;
}

fn required_height(network: Network) -> Height {
    match network {
        Network::Mainnet => *REQUIRED_MAINNET_HEIGHT,
        Network::Testnet => *REQUIRED_TESTNET_HEIGHT,
    }
}

fn required_serving_version(network: Network) -> Version {
    match network {
        Network::Mainnet => Version(170_100),
        Network::Testnet => Version(170_040),
    }
}

fn dns_servable(peer_address: SocketAddr, network: Network) -> bool {
    return peer_address.port() == network.default_port();
}

enum CrawlingMode {
    FastAcquisition,
    LongTermUpdates,
}

#[tokio::main]
async fn main() {
    if let Ok((_softlimit, hardlimit)) = getrlimit(Resource::NOFILE) {
        _ = increase_nofile_limit(hardlimit);
    }
    let max_inflight_conn = match getrlimit(Resource::NOFILE) {
        Ok((softlimit, _hardlimit)) => softlimit.min(4096), // limit it to avoid hurting NAT middleboxes
        _ => 128,                              // if we can't figure it out, play it really safe
    };

    let network = Network::Mainnet;
    let serving_nodes: ServingNodes = Default::default();
    let serving_nodes_shared = Arc::new(RwLock::new(serving_nodes));

    let seedfeed = SeedContext {
        serving_nodes_shared: serving_nodes_shared.clone(),
    };
    let addr = "127.0.0.1:50051".parse().unwrap();
    let seeder_service = Server::builder()
        .add_service(SeederServer::new(seedfeed))
        .serve(addr);
    tokio::spawn(seeder_service);

    let dns_handler = DnsContext {
        serving_nodes_shared: serving_nodes_shared.clone(),
        serving_network: Network::Mainnet,
    };
    let mut dns_server = trust_dns_server::ServerFuture::new(dns_handler);
    let dns_socket = "127.0.0.1:5300".to_socket_addrs().unwrap().next().unwrap();
    dns_server.register_socket(UdpSocket::bind(dns_socket).await.unwrap());
    tokio::spawn(dns_server.block_until_done());

    let initial_peer_addrs = ["35.230.70.77:8233", "157.245.172.190:8233"];
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
                    peers_timeout: Duration::from_secs(32),
                    hash_timeout: Duration::from_secs(32),
                };
                fast_walker(
                    &serving_nodes_shared,
                    &mut internal_peer_tracker,
                    network,
                    timeouts,
                )
                .await;
                {
                    let serving_nodes_testing = serving_nodes_shared.read().unwrap();
                    if serving_nodes_testing.primaries.len() + serving_nodes_testing.alternates.len() > 32 {
                        mode = CrawlingMode::LongTermUpdates;
                    }
                }
            }
            CrawlingMode::LongTermUpdates => {
                let timeouts = Timeouts {
                    peers_timeout: Duration::from_secs(32),
                    hash_timeout: Duration::from_secs(32),
                };
                slow_walker(
                    &serving_nodes_shared,
                    &mut internal_peer_tracker,
                    network,
                    max_inflight_conn.try_into().unwrap(),
                    timeouts,
                )
                .await;
            }
        }
        // just in case...we could add code to check if this does anything to find bugs with the incremental update
        update_serving_nodes(&serving_nodes_shared, &internal_peer_tracker);
        sleep(Duration::new(2, 0)).await;
        println!("done with getting results");
    }
}

fn update_serving_nodes(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &HashMap<SocketAddr, Option<PeerStats>>,
) {
    let mut primary_nodes = HashSet::new();
    let mut alternate_nodes = HashSet::new();

    for (key, value) in internal_peer_tracker {
        let classification = get_classification(&value, &key, Network::Mainnet);
        if classification == PeerClassification::AllGood {
            primary_nodes.insert(key.clone());
        }
        if classification == PeerClassification::MerelySyncedEnough {
            alternate_nodes.insert(key.clone());
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

fn single_node_update(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    new_peer_address: &SocketAddr,
    new_peer_stat: &Option<PeerStats>,
) {
    let old_nodes = serving_nodes_shared.read().unwrap();
    let mut primary_nodes = old_nodes.primaries.clone();
    let mut alternate_nodes = old_nodes.alternates.clone();
    drop(old_nodes);

    match get_classification(new_peer_stat, new_peer_address, Network::Mainnet) {
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

struct Timeouts {
    hash_timeout: Duration,
    peers_timeout: Duration,
}
async fn probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
) -> (SocketAddr, Option<PeerStats>) {
    // we always return the SockAddr of the server we probed, so we can reissue queries
    let mut new_peer_stats = match old_stats {
        None => PeerStats {
            total_attempts: 0,
            total_successes: 0,
            ewma_pack: EWMAPack::default(),
            last_polled: None,
            last_success: None,
            peer_derived_data: None,
        },
        Some(old_stats) => old_stats.clone(),
    };
    sleep(random_delay).await;
    let current_poll_time = SystemTime::now(); // sample time here, in case peer req takes a while
    let poll_res = test_a_server(proband_address, network, timeouts.hash_timeout).await;
    //println!("result = {:?}", poll_res);
    new_peer_stats.total_attempts += 1;
    match poll_res {
        PollStatus::RetryConnection() => {
            println!("Retry the connection!");
            return (proband_address, None);
        }
        PollStatus::BlockRequestOK(new_peer_data) => {
            new_peer_stats.total_successes += 1;
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            new_peer_stats.last_success = Some(SystemTime::now());
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                true,
            );
        }
        PollStatus::BlockRequestFail(new_peer_data) => {
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                false,
            );
        }
        PollStatus::ConnectionFail() => {
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                false,
            );
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (
        proband_address,
        Some(new_peer_stats),
    );
}

async fn slow_walker(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &mut HashMap<SocketAddr, Option<PeerStats>>,
    network: Network,
    max_inflight_conn: usize,
    timeouts: Timeouts,
) {
    let mut rng = rand::thread_rng();
    let mut batch_queries = Vec::new();
    for (proband_address, peer_stat) in internal_peer_tracker.iter() {
        if poll_this_time_around(peer_stat, proband_address, network) {
            batch_queries.push(probe_and_update(
                proband_address.clone(),
                peer_stat.clone(),
                network,
                &timeouts,
                Duration::from_secs(rng.gen_range(0..256)),
            ));
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
        if let Some(new_peer_stat) = probe_result.1 {
            println!("{:?} has new peer stat: {:?}", peer_address, new_peer_stat);
            //println!("new peers: {:?}", new_peers);
            let new_peer_stat = new_peer_stat.clone();
            internal_peer_tracker.insert(peer_address.clone(), Some(new_peer_stat.clone()));
            single_node_update(&serving_nodes_shared, &peer_address, &Some(new_peer_stat));
            println!("HashMap len: {:?}", internal_peer_tracker.len());
        } else {
            println!("SLOW WALKER MUST RETRY {:?} NEXT TIME AROUND", peer_address);
        }
        let peers_res = probe_for_peers(peer_address, network, timeouts.peers_timeout).await;
        if let Some(peer_list) = peers_res {
            for peer in peer_list {
                let key = peer.addr().to_socket_addrs().unwrap().next().unwrap();
                if !internal_peer_tracker.contains_key(&key) {
                    internal_peer_tracker.insert(key.clone(), <Option<PeerStats>>::None);
                }
            }
        }
    }
}

async fn fast_walker(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &mut HashMap<SocketAddr, Option<PeerStats>>,
    network: Network,
    timeouts: Timeouts,
) {
    let mut handles = FuturesUnordered::new();
    let mut rng = rand::thread_rng();
    for (proband_address, peer_stat) in internal_peer_tracker.iter() {
        handles.push(probe_and_update(
            proband_address.clone(),
            peer_stat.clone(),
            network,
            &timeouts,
            Duration::from_secs(rng.gen_range(0..64)),
        ));
    }
    while let Some(probe_result) = handles.next().await {
        let peer_address = probe_result.0;
        if let Some(new_peer_stat) = probe_result.1 {
            println!("{:?} has new peer stat: {:?}", peer_address, new_peer_stat);
            //println!("new peers: {:?}", new_peers);
            let new_peer_stat = new_peer_stat.clone();
            internal_peer_tracker.insert(peer_address.clone(), Some(new_peer_stat.clone()));
            single_node_update(&serving_nodes_shared, &peer_address, &Some(new_peer_stat));
            println!("HashMap len: {:?}", internal_peer_tracker.len());
        } else {
            // we gotta retry
            println!("RETRYING {:?}", peer_address);
            handles.push(probe_and_update(
                peer_address.clone(),
                internal_peer_tracker[&peer_address].clone(),
                network,
                &timeouts,
                Duration::from_secs(rng.gen_range(0..64)),
            ))
        }
        let peers_res = probe_for_peers(peer_address, network, timeouts.peers_timeout).await;
        if let Some(peer_list) = peers_res {
            for peer in peer_list {
                let key = peer.addr().to_socket_addrs().unwrap().next().unwrap();
                if !internal_peer_tracker.contains_key(&key) {
                    internal_peer_tracker.insert(key.clone(), <Option<PeerStats>>::None);
                    handles.push(probe_and_update(
                        key.clone(),
                        <Option<PeerStats>>::None,
                        network,
                        &timeouts,
                        Duration::from_secs(rng.gen_range(0..64)),
                    ));
                }
            }
        }
    }
}
