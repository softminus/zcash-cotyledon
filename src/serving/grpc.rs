use grpc_protocol::{SeedReply, SeedRequest};
use crate::serving::ServingNodes;
use grpc_protocol::seeder_server::Seeder;
use futures::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use rand::Rng;
use rlimit::{getrlimit, increase_nofile_limit, Resource};
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
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_chain::serialization::SerializationError;
use zebra_consensus::CheckpointList;
use zebra_network::types::{MetaAddr, PeerServices};
use zebra_network::{
    connect_isolated_tcp_direct, HandshakeError, InventoryResponse, Request, Response, Version,
};


pub mod grpc_protocol {
    tonic::include_proto!("seeder"); // The string specified here must match the proto package name
}


#[derive(Debug)]
pub struct SeedContext {
    pub serving_nodes_shared: Arc<RwLock<ServingNodes>>,
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

        let reply = grpc_protocol::SeedReply {
            primaries: primary_nodes_strings,
            alternates: alternate_nodes_strings,
        };

        Ok(TonicResponse::new(reply)) // Send back our formatted greeting
    }
}
