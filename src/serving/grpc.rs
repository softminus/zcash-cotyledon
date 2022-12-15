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
