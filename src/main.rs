#![feature(type_name_of_val)]
#![feature(ip)]

use futures_util::StreamExt;
use hex::FromHex;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, ToSocketAddrs, IpAddr};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};
use tower::Service;
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_network::types::PeerServices;
use zebra_network::{connect_isolated_tcp_direct, InventoryResponse, Request, Response, Version};
//use zebra_network::protocol::external::types::Version;
use tonic::transport::Server;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use trust_dns_server::{authority::MessageResponseBuilder, client::rr as dnsrr, proto::op as dnsop, server as dns};
use tokio::net::UdpSocket;
use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedReply, SeedRequest};
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
        response: R,
    ) -> dns::ResponseInfo {
        match self.do_handle_request(request, response).await {
            Some(response_info) => response_info,
            None => {
                println!("unable to respond to query {:?}", request);
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
            return None;
        }
        if request.message_type() != dnsop::MessageType::Query {
            return None;
        }

        let endpoint = dnsrr::LowerName::from(dnsrr::Name::from_str("dnsseed.z.cash").unwrap());

        println!("query is {:?}", endpoint.zone_of(request.query().name()));

        let builder = MessageResponseBuilder::from_message_request(request);
        let mut header = dnsop::Header::response_from_request(request.header());
        header.set_authoritative(true);
        let mut records = Vec::new();
        {
            let serving_nodes = self.serving_nodes_shared.read().unwrap();

            for peer in serving_nodes.primaries.iter().chain(serving_nodes.alternates.iter()) {
                if dns_servable(*peer, self.serving_network) {
                    let rdata = match peer.ip() {
                        IpAddr::V4(ipv4) => dnsrr::RData::A(ipv4),
                        IpAddr::V6(ipv6) => dnsrr::RData::AAAA(ipv6),
                    };
                    records.push(dnsrr::Record::from_rdata(request.query().name().into(), 60, rdata))
                }
            }
        }
        let response = builder.build(header, records.iter(), &[], &[], &[]);
        Some(response_handle.send_response(response).await.unwrap()) // fixme, handle failure OK.
    }
}
#[derive(Debug, Clone)]
enum PollStatus {
    ConnectionFail(),
    BlockRequestFail(PeerDerivedData),
    BlockRequestOK(PeerDerivedData),
}
#[derive(Debug, Clone)]
struct PeerDerivedData {
    numeric_version: Version,
    peer_services: PeerServices,
    peer_height: Height,
    _user_agent: String,
    _relay: bool,
}

async fn test_a_server(peer_addr: SocketAddr) -> PollStatus {
    let hash_to_test = "000000000145f21eabd0024fbbb00384111644a5415b02bfe169b4fc300290e6";
    println!("Starting new connection: peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(
        Network::Mainnet,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let x = the_connection.await;
    let mut proband_hash_set = HashSet::new();
    let proband_hash = <Hash>::from_hex(hash_to_test).expect("hex string failure");
    proband_hash_set.insert(proband_hash);
    match x {
        Ok(mut z) => {
            let numeric_version = z.connection_info.remote.version;
            let peer_services = z.connection_info.remote.services;
            let peer_height = z.connection_info.remote.start_height;
            let _user_agent = z.connection_info.remote.user_agent.clone();
            let _relay = z.connection_info.remote.relay;
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

            let resp = z.call(Request::BlocksByHash(proband_hash_set)).await;
            match resp {
                Ok(good_result) => {
                    if let Response::Blocks(block_vector) = good_result {
                        for block_k in block_vector {
                            if let InventoryResponse::Available(actual_block) = block_k {
                                if actual_block.hash() == Hash::from_str(hash_to_test).unwrap() {
                                    println!(
                                        "blocks with {:?} by hash good: {}",
                                        peer_addr,
                                        actual_block.hash()
                                    );
                                    return PollStatus::BlockRequestOK(peer_derived_data);
                                }
                            }
                        }
                    }
                    return PollStatus::BlockRequestFail(peer_derived_data); // it didn't hash so well
                }
                Err(error) => {
                    println!("blocks with {:?} by hash error: {}", peer_addr, error);
                    return PollStatus::BlockRequestFail(peer_derived_data);
                }
            }
        } // ok connect
        Err(error) => {
            println!("Connection with {:?} failed: {:?}", peer_addr, error);
            return PollStatus::ConnectionFail();
        } // failed connect
    };
}

async fn probe_for_peers(peer_addr: SocketAddr) -> Option<Vec<MetaAddr>> {
    println!("Starting new connection: peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(
        Network::Mainnet,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let x = the_connection.await;
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
    MerelySyncedEnough, // Node recently could serve us a recent-enough block (it is syncing or has synced to zcash chain)
    // but doesn't meet uptime criteria.
    AllGood, // Node meets all the legacy criteria (including uptime), and is fully servable to clients
    Bad,     // Node is bad for some reason
    Unknown, // We got told about this node but haven't yet queried it
}

#[derive(Debug, Clone)]
struct PeerStats {
    total_attempts: i32,
    total_successes: i32,
    ewma_pack: EWMAPack,
    last_polled: Instant,
    last_success: Option<Instant>,
    last_polled_absolute: SystemTime,

    peer_derived_data: Option<PeerDerivedData>,
}

#[derive(Debug, Clone, Default)]
struct ServingNodes {
    primaries: Vec<SocketAddr>,
    alternates: Vec<SocketAddr>,
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

fn update_ewma_pack(prev: &mut EWMAPack, last_polled: Instant, sample: bool) {
    let current = Instant::now();
    let sample_age = current.duration_since(last_polled);
    update_ewma(&mut prev.stat_2_hours, sample_age, sample);
    update_ewma(&mut prev.stat_8_hours, sample_age, sample);
    update_ewma(&mut prev.stat_1day,    sample_age, sample);
    update_ewma(&mut prev.stat_1week,   sample_age, sample);
    update_ewma(&mut prev.stat_1month,  sample_age, sample);
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
        return PeerClassification::Bad;
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
    if ewmas.stat_1day.reliability    > 0.55 && ewmas.stat_1day.count    > 8.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_1week.reliability   > 0.45 && ewmas.stat_1week.count   > 16.0 {
        return PeerClassification::AllGood;
    };
    if ewmas.stat_1month.reliability  > 0.35 && ewmas.stat_1month.count  > 32.0 {
        return PeerClassification::AllGood;
    };




    // at least one success in the last 2 hours
    if let Some(last_success) = peer_stats.last_success {
        if last_success.elapsed() <= Duration::from_secs(60*60*2) {
            return PeerClassification::MerelySyncedEnough;
        }
    }
    return PeerClassification::Bad;
}

fn required_height(network: Network) -> Height {
    match network {
        Network::Mainnet => Height(1759558),
        Network::Testnet => Height(1982923),
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
    LongTerm
}

#[tokio::main]
async fn main() {
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
        slow_walker(&mut internal_peer_tracker);
        println!("done with getting results");
        let mut primary_nodes = Vec::new();
        let mut alternate_nodes = Vec::new();

        for (key, value) in &internal_peer_tracker {
            let classification = get_classification(value, key, Network::Mainnet);
            if classification == PeerClassification::AllGood {
                primary_nodes.push(key.clone());
            }
            if classification == PeerClassification::MerelySyncedEnough {
                alternate_nodes.push(key.clone());
            }
        }
        println!("primary nodes: {:?}", primary_nodes);
        println!("alternate nodes: {:?}", alternate_nodes);
        let new_nodes = ServingNodes {
            primaries: primary_nodes,
            alternates: alternate_nodes,
        };
        {
            let mut unlocked = serving_nodes_shared.write().unwrap();
            *unlocked = new_nodes.clone();
        }
    }
}

async fn probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
) -> (Option<PeerStats>, SocketAddr, Vec<SocketAddr>) {
    let mut new_peer_stats = match old_stats {
        None => PeerStats {
            total_attempts: 0,
            total_successes: 0,
            ewma_pack: EWMAPack::default(),
            last_polled: Instant::now(),
            last_success: None,
            last_polled_absolute: SystemTime::now(),
            peer_derived_data: None,
        },
        Some(old_stats) => old_stats.clone(),
    };
    let mut found_peer_addresses = Vec::new();
    let poll_time = Instant::now();
    let poll_res = test_a_server(proband_address).await;
    let peers_res = probe_for_peers(proband_address).await;
    if let Some(peer_list) = peers_res {
        for peer in peer_list {
            found_peer_addresses.push(peer.addr());
        }
        //println!("found new peers: {:?}", found_peer_addresses);
    }
    //println!("result = {:?}", poll_res);
    new_peer_stats.total_attempts += 1;
    match poll_res {
        PollStatus::BlockRequestOK(new_peer_data) => {
            new_peer_stats.total_successes += 1;
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            new_peer_stats.last_success = Some(Instant::now());
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                true,
            );
        }
        PollStatus::BlockRequestFail(new_peer_data) => {
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                false,
            );
        }
        PollStatus::ConnectionFail() => {
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                false,
            );
        }
    }
    new_peer_stats.last_polled_absolute = SystemTime::now();
    new_peer_stats.last_polled = poll_time;
    return (Some(new_peer_stats), proband_address, found_peer_addresses);
}




async fn slow_walker(internal_peer_tracker: &mut HashMap<SocketAddr, Option<PeerStats>>) {
    let mut batch_queries = Vec::new();
    for (proband_address, peer_stat) in internal_peer_tracker.iter() {
        if let Some(unwrapped_stats) = peer_stat {
            if !unwrapped_stats.peer_derived_data.is_none() {
                println!("Prioritized node query for {:?}", proband_address);
                batch_queries.insert(
                    0,
                    probe_and_update(proband_address.clone(), peer_stat.clone()),
                );
            }
        } else {
            batch_queries.push(probe_and_update(proband_address.clone(), peer_stat.clone()));
        }
    }

    let mut stream = futures::stream::iter(batch_queries).buffer_unordered(1024);

    println!("now let's make them run");

    while let Some(probe_result) = stream.next().await {
        //println!("probe_result {:?}", probe_result);

        let new_peer_stat = probe_result.0.clone();
        let peer_address = probe_result.1;
        let new_peers = probe_result.2;
        println!("RESULT {} {:?}", peer_address, new_peer_stat);
        internal_peer_tracker.insert(peer_address, new_peer_stat);
        for peer in new_peers {
            let key = peer.to_socket_addrs().unwrap().next().unwrap();
            if !internal_peer_tracker.contains_key(&key) {
                internal_peer_tracker.insert(key.clone(), <Option<PeerStats>>::None);
            }
        }

        println!("HashMap len: {:?}", internal_peer_tracker.len());
    }
}