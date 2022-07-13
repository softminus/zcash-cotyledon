#![feature(type_name_of_val)]

use zebra_network::init;

use std::time::Duration;
use zebra_chain::{chain_tip::NoChainTip, parameters::Network};
use tokio::{pin, select, sync::oneshot};
use tokio::runtime::Runtime;

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

#[derive(Debug)]
struct PeerStats {
    address: SocketAddr,
    attempts: i32,
    successes: i32,
}

#[tokio::main]
async fn main()
{
//    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(34, 127, 5, 144)), 8233);
    //let peer_addr = "157.245.172.190:8233".to_socket_addrs().unwrap().next().unwrap();
    let peer_addrs = ["34.127.5.144:8233", "157.245.172.190:8233"];
    let mut peer_tracker = Vec::new();
    
    for peer in peer_addrs {
        let i = PeerStats {address: peer.to_socket_addrs().unwrap().next().unwrap(),
            attempts: 0,
            successes: 0};
        peer_tracker.push(i);
    }
    loop {
        for peer in peer_tracker.iter_mut() {
            let poll_res = test_a_server(peer.address).await;
            println!("result = {:?}", poll_res);
            peer.attempts += 1;
            match poll_res {
                PollResult::PollOK => {peer.successes += 1}
                _ => {}
            }
            sleep(Duration::new(4,0));
            println!("updated peer stats = {:?}", peer);
        }
    }
    // .to_socket_addrs().unwrap().next().unwrap();
    // loop {
    //     //let peer_addr = SocketAddr::new(proband_ip, proband_port);
    //     test_a_server(peer_addr).await;
    //     sleep(Duration::new(5, 0));
    // }
    
}