#![feature(type_name_of_val)]

use zebra_network::init;

use std::time::Duration;
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
use std::time::Instant;
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

#[derive(Debug, Clone, Copy)]
struct EWMAState {
    scale:       Duration,
    weight:      f64,
    count:       f64,
    reliability: f64,
}

#[derive(Debug, Clone)]
struct PeerStats {
    address: SocketAddr,
    attempts: i32,
    successes: i32,
    uptimes: Vec<EWMAState>
}



fn update_EWMA(prev: &mut EWMAState, sample_age: Duration, sample: bool) {
    let weight_factor = (-sample_age.as_secs_f64()/prev.scale.as_secs_f64()).exp();

    let sample_value:f64 = sample as i32 as f64;

    let reliability:f64 = prev.reliability * weight_factor + sample_value * (1.0-weight_factor);

    // I don't understand what this and the following line do
    let count:f64 = prev.count * weight_factor + 1.0;

    let weight:f64 = prev.weight * weight_factor + (1.0-weight_factor);

    prev.weight = weight;
    prev.count = count;
    prev.reliability = reliability;
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
            attempts: 0,
            successes: 0};
        internal_peer_tracker.push(i);
    }

    loop {
        for peer in internal_peer_tracker.iter_mut() {
            let poll_res = test_a_server(peer.address).await;
            println!("result = {:?}", poll_res);
            peer.attempts += 1;
            match poll_res {
                PollResult::PollOK => {peer.successes += 1}
                _ => {}
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