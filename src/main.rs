#![feature(type_name_of_val)]
#![feature(ip)]

use std::time::{Duration, Instant, SystemTime};
use zebra_chain::{parameters::Network};
use std::sync::Mutex;
use std::{
    sync::Arc,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio::sync::mpsc::unbounded_channel;

use std::collections::{HashSet, HashMap};
use futures_util::StreamExt;
use tower::Service;
use zebra_chain::block::Hash;
use zebra_network::{connect_isolated_tcp_direct, Request};
use zebra_network::{
    types::{AddrInVersion, Nonce, PeerServices},
    ConnectedAddr, ConnectionInfo, Version, VersionMessage,
};
use zebra_chain::block::Height;
use std::thread::sleep;
use std::net::{SocketAddr, ToSocketAddrs};
use hex::FromHex;
//use zebra_network::protocol::external::types::Version;
use tonic::{transport::Server, Request as TonicRequest, Response as TonicResponse, Status};

use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedRequest, SeedReply};
use zebra_network::types::MetaAddr;
pub mod seeder_proto {
    tonic::include_proto!("seeder"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct SeedContext {
    peer_tracker_shared: Arc<Mutex<Vec<PeerStats>>>
}

// #[tonic::async_trait]
// impl Seeder for SeedContext {
//     async fn seed(
//         &self,
//         request: TonicRequest<SeedRequest>, // Accept request of type SeedRequest
//     ) -> Result<TonicResponse<SeedReply>, Status> { // Return an instance of type SeedReply
//         println!("Got a request: {:?}", request);
//         let peer_tracker_shared = self.peer_tracker_shared.lock().unwrap();
//         let mut peer_strings = Vec::new();
// //        for peer in peer_tracker_shared.iter() {
// //            peer_strings.push(format!("{:?}",peer.address))
// //        }
//         let reply = seeder_proto::SeedReply {
//             ip: peer_strings
//         };

//         Ok(TonicResponse::new(reply)) // Send back our formatted greeting
//     }
// }




#[derive(Debug, Clone)]
enum PollStatus {
    ConnectionFail(),
    BlockRequestFail(PeerDerivedData),
    BlockRequestOK(PeerDerivedData)
}
#[derive(Debug, Clone)]
struct PeerDerivedData {
    numeric_version: Version,
    peer_services:   PeerServices,
    peer_height:     Height,
    user_agent:      String,
    relay:           bool,
}

async fn test_a_server(peer_addr: SocketAddr) -> PollStatus
{
    println!("Starting new connection: peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(Network::Mainnet, peer_addr, String::from("/Seeder-and-feeder:0.0.0-alpha0/"));
    let x = the_connection.await;
    let mut proband_hash_set = HashSet::new();
    let proband_hash = <Hash>::from_hex("000000000145f21eabd0024fbbb00384111644a5415b02bfe169b4fc300290e6").expect("hex string failure");
    proband_hash_set.insert(proband_hash);
    match x {
        Ok(mut z) => {
            let numeric_version = z.connection_info.remote.version;
            let peer_services = z.connection_info.remote.services;
            let peer_height = z.connection_info.remote.start_height;
            let user_agent = z.connection_info.remote.user_agent.clone();
            let relay = z.connection_info.remote.relay;
            let peer_derived_data = PeerDerivedData {numeric_version, peer_services, peer_height, user_agent, relay};
            // println!("remote peer version: {:?}", z.connection_info.remote.version >= Version(170_100));
            // println!("remote peer services: {:?}", z.connection_info.remote.services.intersects(PeerServices::NODE_NETWORK));
            // println!("remote peer height @ time of connection: {:?}", z.connection_info.remote.start_height >= Height(1_700_000));

            let resp = z.call(Request::BlocksByHash(proband_hash_set)).await;
            match resp {
                Ok(res) => {
                println!("blocks with {:?} by hash good: {}", peer_addr, res);
                return PollStatus::BlockRequestOK(peer_derived_data)
            }
                Err(error) => {
                println!("blocks with {:?} by hash error: {}", peer_addr, error);
                return PollStatus::BlockRequestFail(peer_derived_data)
            }
            }
        } // ok connect
        Err(error) => {
            println!("Connection with {:?} failed: {:?}",peer_addr, error);
            return PollStatus::ConnectionFail();
        } // failed connect
    };
}



async fn probe_for_peers(peer_addr: SocketAddr) -> Option<Vec<MetaAddr>>
{
    println!("Starting new connection: peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(Network::Mainnet, peer_addr, String::from("/Seeder-and-feeder:0.0.0-alpha0/"));
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
            println!("Peers connection with {:?} failed: {:?}",peer_addr, error);
            return None;
        }
    };
}


//Connection with 74.208.91.217:8233 failed: Serialization(Parse("getblocks version did not match negotiation"))


#[derive(Debug, Clone, Copy, Default)]
struct EWMAState {
    scale:       Duration,
    weight:      f64,
    count:       f64,
    reliability: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PeerClassification {
    MerelySyncedEnough, // Node recently could serve us a recent-enough block (it is syncing or has synced to zcash chain)
                        // but doesn't meet uptime criteria.
    AllGood,            // Node meets all the legacy criteria (including uptime), and is fully servable to clients
    Bad,                // Node is bad for some reason
    Unknown             // We got told about this node but haven't yet queried it
}


#[derive(Debug, Clone)]
struct PeerStats {
    total_attempts: i32,
    total_successes: i32,
    ewma_pack: EWMAPack,
    last_polled: Instant,
    last_polled_absolute: SystemTime,

    peer_derived_data: Option<PeerDerivedData>
}


#[derive(Debug, Clone, Copy)]
struct EWMAPack{
    stat_2_hours: EWMAState,
    stat_8_hours: EWMAState,
    stat_1day: EWMAState,
    stat_1week: EWMAState,
    stat_1month: EWMAState
}


impl Default for EWMAPack {
    fn default() -> Self { EWMAPack {
        stat_2_hours: EWMAState {scale: Duration::new(3600*2,0),     ..Default::default()},
        stat_8_hours: EWMAState {scale: Duration::new(3600*8,0),     ..Default::default()},
        stat_1day:    EWMAState {scale: Duration::new(3600*24,0),    ..Default::default()},
        stat_1week:   EWMAState {scale: Duration::new(3600*24*7,0),  ..Default::default()},
        stat_1month:  EWMAState {scale: Duration::new(3600*24*30,0), ..Default::default()}
    }
    }
}
fn update_ewma(prev: &mut EWMAState, sample_age: Duration, sample: bool) {
    let weight_factor = (-sample_age.as_secs_f64()/prev.scale.as_secs_f64()).exp();

    let sample_value:f64 = sample as i32 as f64;
    //println!("sample_value is: {}, weight_factor is {}", sample_value, weight_factor);
    prev.reliability = prev.reliability * weight_factor + sample_value * (1.0-weight_factor);

    // I don't understand what this and the following line do
    prev.count = prev.count * weight_factor + 1.0;

    prev.weight = prev.weight * weight_factor + (1.0-weight_factor);
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


fn get_classification(peer_stats: &Option<PeerStats>, peer_address: &SocketAddr, network: Network) -> PeerClassification {

    let peer_stats = match peer_stats {
        None => return PeerClassification::Unknown,
        Some(peer_stats) => peer_stats,
    };

    if peer_stats.total_attempts == 0 { // we never pinged them
        return PeerClassification::Unknown;
    }

    if peer_stats.peer_derived_data.is_none() {
        // we tried them, but haven't been able to negotiate a connection
        return PeerClassification::Bad;
    }

    let peer_derived_data = peer_stats.peer_derived_data.as_ref().unwrap();

    if !peer_derived_data.peer_services.intersects(PeerServices::NODE_NETWORK) {
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
    if peer_stats.total_attempts <= 3 && peer_stats.total_successes * 2 >= peer_stats.total_attempts {return PeerClassification::AllGood};

    if ewmas.stat_2_hours.reliability > 0.85 && ewmas.stat_2_hours.count > 2.0     {return PeerClassification::AllGood};
    if ewmas.stat_8_hours.reliability > 0.70 && ewmas.stat_8_hours.count > 4.0     {return PeerClassification::AllGood};
    if ewmas.stat_1day.reliability    > 0.55 && ewmas.stat_1day.count    > 8.0     {return PeerClassification::AllGood};
    if ewmas.stat_1week.reliability   > 0.45 && ewmas.stat_1week.count   > 16.0    {return PeerClassification::AllGood};
    if ewmas.stat_1month.reliability  > 0.35 && ewmas.stat_1month.count  > 32.0    {return PeerClassification::AllGood};

    if peer_stats.ewma_pack.stat_2_hours.reliability != 0.0 { // OK response to block hash request in the past 2 hours
        return PeerClassification::MerelySyncedEnough;
    }

    return PeerClassification::Bad;
}

fn required_height(network: Network) -> Height {
    match network {
        Network::Mainnet => {Height(1759558)}
        Network::Testnet => {Height(1982923)}
    }
}

fn required_serving_version(network: Network) -> Version {
    match network {
        Network::Mainnet => {Version(170_100)}
        Network::Testnet => {Version(170_040)}
    }
}


fn dns_servable(peer_address: SocketAddr, network: Network) -> bool {
    return peer_address.port() == network.default_port();
}



#[tokio::main]
async fn main()
{
//    let network = Network::Mainnet;
//    println!("{:?}", required_height(network));
    //let addr = "127.0.0.1:50051".parse().unwrap();
    //let peer_tracker_shared = Arc::new(Mutex::new(Vec::new()));

    // let seedfeed = SeedContext {peer_tracker_shared: peer_tracker_shared.clone()};

    // let seeder_service = Server::builder()
    //     .add_service(SeederServer::new(seedfeed))
    //     .serve(addr);

    // tokio::spawn(seeder_service);

//    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(34, 127, 5, 144)), 8233);
    //let peer_addr = "157.245.172.190:8233".to_socket_addrs().unwrap().next().unwrap();
    let initial_peer_addrs = ["35.230.70.77:8233", "157.245.172.190:8233"];
    let mut internal_peer_tracker = HashMap::new();



    for peer in initial_peer_addrs {
        let key = peer.to_socket_addrs().unwrap().next().unwrap();
        let value = None;
        internal_peer_tracker.insert(key, value);
    }
    // for k in internal_peer_tracker.iter_mut().filter(|x| x.peer_classification == PeerClassification::Unknown) {
    //     k.peer_classification = PeerClassification::ActiveQuery;
    // }
    for _ in 1..3 {
    //    let mut handles = FuturesUnordered::new();
        println!("starting Loop");

        let (tx, rx) = unbounded_channel();

        for (proband_address, peer_stat) in &internal_peer_tracker {
            tx.send(probe_and_update(proband_address.clone(), peer_stat.clone()));
        }
        println!("now let's make them run");
        let receiver_stream = UnboundedReceiverStream::new(rx);
        let mut buffered = receiver_stream.buffer_unordered(1024);

        while let Some(probe_result) = buffered.next().await {
            //println!("probe_result {:?}", probe_result);

            let new_peer_stat = probe_result.0.clone();
            let peer_address  = probe_result.1;
            let new_peers = probe_result.2;
            println!("RESULT {:?}",new_peer_stat);
            internal_peer_tracker.insert(peer_address, new_peer_stat);
            for peer in new_peers {
                let key = peer.to_socket_addrs().unwrap().next().unwrap();
                if !internal_peer_tracker.contains_key(&key) {
                    let value = <Option<PeerStats>>::None;
                    internal_peer_tracker.insert(key.clone(), value.clone());
                    //tx.send(probe_and_update(key.clone(), value.clone()));
                }
            }

        println!("HashMap len: {:?}", internal_peer_tracker.len());
        }
        println!("done with getting results");

        //sleep(Duration::new(4, 0));
        let mut top_tier_nodes = Vec::new();
        let mut contingency_nodes = Vec::new();

        for (key, value) in &internal_peer_tracker {
            let classification = get_classification(value, key, Network::Mainnet);
            if classification == PeerClassification::AllGood {
                top_tier_nodes.push(key);
            }
            if classification == PeerClassification::MerelySyncedEnough {
                contingency_nodes.push(key);
            }
        }
        println!("best nodes: {:?}", top_tier_nodes);
        println!("contingency nodes: {:?}", contingency_nodes);
    }

        //println!("{:?}", internal_peer_tracker);

        // let mut unlocked = peer_tracker_shared.lock().unwrap();
        // *unlocked = internal_peer_tracker.clone();
        // std::mem::drop(unlocked);

    //}
    // .to_socket_addrs().unwrap().next().unwrap();
    // loop {
    //     //let peer_addr = SocketAddr::new(proband_ip, proband_port);
    //     test_a_server(peer_addr).await;
    //     sleep(Duration::new(5, 0));
    // }
    
}






async fn probe_and_update(proband_address: SocketAddr, old_stats: Option<PeerStats>) -> (Option<PeerStats>, SocketAddr, Vec<SocketAddr>) {
    let mut new_peer_stats = match old_stats {
        None => PeerStats {
            total_attempts: 0,
            total_successes: 0,
            ewma_pack: EWMAPack::default(),
            last_polled: Instant::now(),
            last_polled_absolute: SystemTime::now(),
            peer_derived_data: None
        },
        Some(old_stats) => old_stats.clone()
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
            update_ewma_pack(&mut new_peer_stats.ewma_pack, new_peer_stats.last_polled, true);
        }
        PollStatus::BlockRequestFail(new_peer_data) => {
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            update_ewma_pack(&mut new_peer_stats.ewma_pack, new_peer_stats.last_polled, false);
        }
        PollStatus::ConnectionFail() => {
            update_ewma_pack(&mut new_peer_stats.ewma_pack, new_peer_stats.last_polled, false);
        }

    }
    new_peer_stats.last_polled_absolute = SystemTime::now();
    new_peer_stats.last_polled = poll_time;
    return (Some(new_peer_stats), proband_address, found_peer_addresses);

}