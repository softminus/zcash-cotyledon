#![feature(type_name_of_val)]

use zebra_network::init;

use std::time::{Duration, Instant, SystemTime};
use zebra_chain::{chain_tip::NoChainTip, parameters::Network};
use tokio::{pin, select, sync::oneshot};
use tokio::runtime::Runtime;
use std::sync::Mutex;
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    future::{FutureExt, TryFutureExt},
    stream::Stream,
};
use chrono::Utc;
use tokio::sync::oneshot::error::TryRecvError;

use std::net::{IpAddr, Ipv4Addr};


use tower::{builder::ServiceBuilder, buffer::Buffer, timeout::Timeout, util::BoxService, Service, ServiceExt, service_fn};

use zebra_network as zn;
use zebra_state as zs;
use zn::Response;
use zebra_chain::{
    block::{self, Block},
    transaction::UnminedTxId,
};
use zebra_network::{
    constants::{ADDR_RESPONSE_LIMIT_DENOMINATOR, MAX_ADDRS_IN_MESSAGE},
    AddressBook, InventoryResponse, Config, connect_isolated_tcp_direct, Request
};

use zebra_network::types::PeerServices;
use zebra_chain::serialization::DateTime32;
use zebra_network::PeerAddrState;
use std::thread::sleep;
use std::net::{SocketAddr, ToSocketAddrs};


use tonic::{transport::Server, Request as TonicRequest, Response as TonicResponse, Status};

use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedRequest, SeedReply};

pub mod seeder_proto {
    tonic::include_proto!("seeder"); // The string specified here must match the proto package name
}

#[derive(Debug)]
pub struct SeedContext {
    peer_tracker_shared: Arc<Mutex<Vec<PeerStats>>>
}

#[tonic::async_trait]
impl Seeder for SeedContext {
    async fn seed(
        &self,
        request: TonicRequest<SeedRequest>, // Accept request of type SeedRequest
    ) -> Result<TonicResponse<SeedReply>, Status> { // Return an instance of type SeedReply
        println!("Got a request: {:?}", request);
        let peer_tracker_shared = self.peer_tracker_shared.lock().unwrap();
        let mut peer_strings = Vec::new();
        for peer in peer_tracker_shared.iter() {
            peer_strings.push(format!("{:?}",peer.address))
        }
        let reply = seeder_proto::SeedReply {
            ip: peer_strings
        };

        Ok(TonicResponse::new(reply)) // Send back our formatted greeting
    }
}




#[derive(Debug)]
enum PollResult {
    ConnectionFail,
    RequestFail,
    PollOK
}

async fn test_a_server(peer_addr: SocketAddr) -> PollResult
{
    println!("peer addr is {:?}", peer_addr);
    let the_connection = connect_isolated_tcp_direct(Network::Mainnet, peer_addr, String::from("/Seeder-and-feeder:0.0.0-alpha0/"));
    let x = the_connection.await;

    match x {
        Ok(mut z) => {
            let resp = z.call(Request::Peers).await;
            match resp {
                Ok(res) => {
                println!("peers response: {}", res);
                return PollResult::PollOK;
            }
                Err(error) => {
                println!("peer error: {}", error);
                return PollResult::RequestFail;
            }
            }
        }



        Err(error) => {
            println!("Connection failed: {:?}", error);
            return PollResult::ConnectionFail;
        }
    };
    println!("seem to be done with the connection...");
}

#[derive(Debug, Clone, Copy, Default)]
struct EWMAState {
    scale:       Duration,
    weight:      f64,
    count:       f64,
    reliability: f64,
}

#[derive(Debug, Clone)]
struct PeerStats {
    address: SocketAddr,
    total_attempts: i32,
    total_successes: i32,
    uptimes: EWMAPack,
    last_polled: Instant,
    last_polled_absolute: SystemTime
}

#[derive(Debug, Clone, Copy)]
struct EWMAPack{
    stat2H: EWMAState,
    stat8H: EWMAState,
    stat1D: EWMAState,
    stat1W: EWMAState,
    stat1M: EWMAState
}

impl Default for EWMAPack {
    fn default() -> Self { EWMAPack {
        stat2H: EWMAState {scale: Duration::new(3600*2,0), ..Default::default()},
        stat8H: EWMAState {scale: Duration::new(3600*8,0), ..Default::default()},
        stat1D: EWMAState {scale: Duration::new(3600*24,0), ..Default::default()},
        stat1W: EWMAState {scale: Duration::new(3600*24*7,0), ..Default::default()},
        stat1M: EWMAState {scale: Duration::new(3600*24*30,0), ..Default::default()}
    }
    }
}
fn update_EWMA(prev: &mut EWMAState, sample_age: Duration, sample: bool) {
    let weight_factor = (-sample_age.as_secs_f64()/prev.scale.as_secs_f64()).exp();

    let sample_value:f64 = sample as i32 as f64;
    println!("sample_value is: {}, weight_factor is {}", sample_value, weight_factor);
    prev.reliability = prev.reliability * weight_factor + sample_value * (1.0-weight_factor);

    // I don't understand what this and the following line do
    prev.count = prev.count * weight_factor + 1.0;

    prev.weight = prev.weight * weight_factor + (1.0-weight_factor);
}

fn update_EWMA_pack(prev: &mut EWMAPack, last_polled: Instant, sample: bool) {
    let current = Instant::now();
    let sample_age = current.duration_since(last_polled);
    update_EWMA(&mut prev.stat2H, sample_age, sample);
    update_EWMA(&mut prev.stat8H, sample_age, sample);
    update_EWMA(&mut prev.stat1D, sample_age, sample);
    update_EWMA(&mut prev.stat1W, sample_age, sample);
    update_EWMA(&mut prev.stat1M, sample_age, sample);
}
#[tokio::main]
async fn main()
{
    let addr = "127.0.0.1:50051".parse().unwrap();
    let peer_tracker_shared = Arc::new(Mutex::new(Vec::new()));

    let seedfeed = SeedContext {peer_tracker_shared: peer_tracker_shared.clone()};

    let seeder_service = Server::builder()
        .add_service(SeederServer::new(seedfeed))
        .serve(addr);

    tokio::spawn(seeder_service);

//    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(34, 127, 5, 144)), 8233);
    //let peer_addr = "157.245.172.190:8233".to_socket_addrs().unwrap().next().unwrap();
    let peer_addrs = ["34.127.5.144:8233", "157.245.172.190:8233"];
    let mut internal_peer_tracker = Vec::new();


    
    for peer in peer_addrs {
        let i = PeerStats {address: peer.to_socket_addrs().unwrap().next().unwrap(),
            total_attempts: 0,
            total_successes: 0,
            uptimes: EWMAPack::default(),
            last_polled: Instant::now(),
            last_polled_absolute: SystemTime::now()};
        internal_peer_tracker.push(i);
    }

    loop {
        for peer in internal_peer_tracker.iter_mut() {
            let poll_time = Instant::now();
            let poll_res = test_a_server(peer.address).await;
            println!("result = {:?}", poll_res);
            peer.total_attempts += 1;
            match poll_res {
                PollResult::PollOK => {
                    peer.total_successes += 1;
                    update_EWMA_pack(&mut peer.uptimes, peer.last_polled, true);
                }
                _ => {
                    update_EWMA_pack(&mut peer.uptimes, peer.last_polled, false);
                }
            }
            println!("updated peer stats = {:?}", peer);
        }
        let mut unlocked = peer_tracker_shared.lock().unwrap();
        *unlocked = internal_peer_tracker.clone();
        std::mem::drop(unlocked);

        sleep(Duration::new(4,0));
    }
    // .to_socket_addrs().unwrap().next().unwrap();
    // loop {
    //     //let peer_addr = SocketAddr::new(proband_ip, proband_port);
    //     test_a_server(peer_addr).await;
    //     sleep(Duration::new(5, 0));
    // }


    
}