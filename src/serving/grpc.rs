use std::sync::{Arc, RwLock};
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};

use crate::serving::ServingNodes;

use grpc_protocol::seeder_server::Seeder;
use grpc_protocol::{SeedReply, SeedRequest};

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
