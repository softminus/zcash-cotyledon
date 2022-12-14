#![feature(ip)]
#![feature(io_error_uncategorized)]
#![feature(io_error_more)]
#![feature(once_cell)]
use futures::Future;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tower::Service;
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_chain::serialization::SerializationError;
use zebra_network::types::PeerServices;
use zebra_network::{
    connect_isolated_tcp_direct, HandshakeError, InventoryResponse, Request, Response, Version,
};
//use zebra_network::protocol::external::types::Version;
use rand::Rng;
use rlimit::{getrlimit, increase_nofile_limit, Resource};
use seeder_proto::seeder_server::{Seeder, SeederServer};
use seeder_proto::{SeedReply, SeedRequest};
use tokio::net::{TcpListener, UdpSocket};
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
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        if request.message_type() != dnsop::MessageType::Query {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        if request.query().query_class() != dnsrr::DNSClass::IN {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        let endpoint = dnsrr::LowerName::from(
            dnsrr::Name::from_str("mainnet-test-seed.electriccoin.co").unwrap(),
        ); // make me configurable
        if *request.query().name() != endpoint {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::NXDomain),
                    )
                    .await
                    .unwrap(),
            );
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
                        }
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

// TCPFailure errors:
// Os { code: 60, kind: TimedOut, message: "Operation timed out" }
// Os { code: 61, kind: ConnectionRefused, message: "Connection refused" }
// Os { code: 65, kind: HostUnreachable, message: "No route to host" }
// Os { code: 51, kind: NetworkUnreachable, message: "Network is unreachable" }

// Elapsed(())
// ConnectionClosed

// Serialization(Io(Os { code: 54, kind: ConnectionReset, message: "Connection reset by peer" }))

// ProtocolBad errors
// it's possible some of the TCPFailure errors indicate a protocol problem but we want to avoid false positives
// Serialization(Parse("getblocks version did not match negotiation"))
// Serialization(Parse("getblocks version did not match negotiation"))
// Serialization(Parse("supplied magic did not meet expectations"))
// ObsoleteVersion(_)

// MustRetry errors:
// Os { code: 49, kind: AddrNotAvailable, message: "Can't assign requested address" }

#[derive(Debug, Clone)]
enum ErrorFlavor {
    Network(String),
    Ephemeral(String),
    Protocol(String),
}

fn classify_zebra_network_errors(
    returned_error: &Box<dyn std::error::Error + std::marker::Send + Sync>,
) -> ErrorFlavor {
    if let Some(io_error) = returned_error.downcast_ref::<std::io::Error>() {
        return match io_error.kind() {
            std::io::ErrorKind::Uncategorized      => {ErrorFlavor::Ephemeral("Uncategorized Io error. Perhaps EMFILES / ENFILES, consider increasing ulimit -n hard limit".to_string())}
            std::io::ErrorKind::AddrNotAvailable   => {ErrorFlavor::Ephemeral("AddrNotAvailable Io error. Consider increasing ephemeral port range or decreasing number of maximum concurrent connections".to_string())}
            std::io::ErrorKind::AddrInUse          => {ErrorFlavor::Ephemeral("AddrInUse Io error. Consider increasing ephemeral port range or decreasing number of maximum concurrent connections".to_string())}
            std::io::ErrorKind::ResourceBusy       => {ErrorFlavor::Ephemeral("ResourceBusy Io error. Consider increasing ephemeral port range, increasing ulimit -n hard limit, or decreasing number of maximum concurrent connections".to_string())}
            std::io::ErrorKind::NetworkDown        => {ErrorFlavor::Ephemeral("NetworkDown Io error".to_string())}
            std::io::ErrorKind::AlreadyExists      => {ErrorFlavor::Ephemeral("AlreadyExists Io error. Consider increasing ephemeral port range, increasing ulimit -n hard limit, or decreasing number of maximum concurrent connections".to_string())}

            _                                      => {ErrorFlavor::Network(format!("IO error: {:?}", io_error))}
        };
    }
    if returned_error.is::<tower::timeout::error::Elapsed>()
        || returned_error.is::<tokio::time::error::Elapsed>()
    {
        return ErrorFlavor::Network(format!(
            "zebra-network timed out during handshake: {:?}",
            returned_error
        ));
    }

    if let Some(handshake_error) = returned_error.downcast_ref::<zebra_network::HandshakeError>() {
        return match handshake_error {
            // absolutely ProtocolBad.
            // no way the network could have caused these.
            HandshakeError::UnexpectedMessage(msg)         => {
                ErrorFlavor::Protocol(format!("zebra-network error: host violated protocol and gave us an unexpected message: {:?}",  msg))
            }
            HandshakeError::NonceReuse                     => {
                ErrorFlavor::Protocol("zebra-network error: host violated protocol and reused a nonce".to_string())
            }
            HandshakeError::ObsoleteVersion(ver)           => {
                ErrorFlavor::Protocol(format!("zebra-network error: host tried to negotiate with obsolete version: {:?}", ver))
            }


            // they might be caused by the remote peer software, but we want to avoid
            // false-positive BeyondUseless determination, so we are nice and classify these as
            // TCPFailures
            HandshakeError::ConnectionClosed               => {
                ErrorFlavor::Network("zebra-network error: closed connection unexpectedly".to_string())
            }
            HandshakeError::Io(inner_io_error)             => {
                ErrorFlavor::Network(format!("zebra-network error: inner Io error during handshake: {:?}", inner_io_error))
            }
            HandshakeError::Timeout                        => {
                ErrorFlavor::Network("zebra-network error: timeout during handshake".to_string())
            }

            // this could go both ways -- if it's an Io error, we are nice and classify this as
            // a TCPFailure, otherwise, it's a ProtocolBad
            HandshakeError::Serialization(inner_ser_error) => {
                match inner_ser_error {
                    SerializationError::Io(inner_io_error) => {
                        ErrorFlavor::Network(format!("zebra-network error: inner Serialization error with an Io error within during handshake: {:?}", inner_io_error))
                    }
                    _ => {
                        ErrorFlavor::Protocol(format!("zebra-network error: inner Serialization error during handshake: {:?}", inner_ser_error))
                    }
                }
            }
        };
    }
    println!(
        "RAN INTO MYSTERY ERROR WE CAN'T CLASSIFY: {:?}",
        returned_error
    );
    ErrorFlavor::Ephemeral(format!(
        "Mysterious error we can't classify: {:?}",
        returned_error
    ))
}

async fn hash_probe_inner(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> BlockProbeResult {
    println!(
        "Starting new hash probe connection: peer addr is {:?}",
        peer_addr
    );
    let connection = connect_isolated_tcp_direct(
        network,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let connection = timeout(connection_timeout, connection);
    let connection = connection.await;

    let hash_checkpoints = match network {
        Network::Mainnet => HASH_CHECKPOINTS_MAINNET.clone(),
        Network::Testnet => HASH_CHECKPOINTS_TESTNET.clone(),
    };

    match connection {
        Err(timeout_error) => {
            println!(
                "Hash test connection with {:?} failed due to user-defined timeout of {:?}: {:?}",
                peer_addr, connection_timeout, timeout_error
            );
            BlockProbeResult::TCPFailure // this counts as network brokenness, should not count as BeyondUseless, just regular Bad
        }
        Ok(connection_might_have_failed) => {
            match connection_might_have_failed {
                Err(connection_failure) => {
                    let error_classification = classify_zebra_network_errors(&connection_failure);

                    match error_classification {
                        ErrorFlavor::Ephemeral(msg) => {
                            println!(
                                "Hash test connection with {:?} got an ephemeral error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::MustRetry
                        }
                        ErrorFlavor::Network(msg) => {
                            println!(
                                "Hash test connection with {:?} got an network error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::TCPFailure
                        }
                        ErrorFlavor::Protocol(msg) => {
                            println!(
                                "Hash test connection with {:?} got an protocol error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::ProtocolBad
                        }
                    }
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
                            // so far the only errors we've seen here look like one of these, and it's fine to ascribe them to BlockRequestFail and not to ProtocolBad
                            // SharedPeerError(ConnectionClosed)
                            // SharedPeerError(ConnectionReceiveTimeout)
                            // SharedPeerError(NotFoundResponse([Block(block::Hash("")), Block(block::Hash(""))]))
                            return BlockProbeResult::BlockRequestFail(peer_derived_data);
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
                                        return BlockProbeResult::BlockRequestOK(peer_derived_data);
                                    }
                                    // node returned a Blocks response, but it wasn't complete/correct for some reason
                                    BlockProbeResult::BlockRequestFail(peer_derived_data)
                                } // Response::Blocks(block_vector)
                                _ => {
                                    // connection established but we didn't get a Blocks response
                                    BlockProbeResult::BlockRequestFail(peer_derived_data)
                                }
                            } // match hash_query_protocol_response
                        } // Ok(hash_query_protocol_response)
                    } // match hash_query_response
                } // Ok(good_connection)
            }
        }
    }
}





async fn connection_probe_inner(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> BlockProbeResult {
    println!(
        "Starting new hash probe connection: peer addr is {:?}",
        peer_addr
    );
    let connection = connect_isolated_tcp_direct(
        network,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let connection = timeout(connection_timeout, connection);
    let connection = connection.await;

    let hash_checkpoints = match network {
        Network::Mainnet => HASH_CHECKPOINTS_MAINNET.clone(),
        Network::Testnet => HASH_CHECKPOINTS_TESTNET.clone(),
    };

    match connection {
        Err(timeout_error) => {
            println!(
                "Hash test connection with {:?} failed due to user-defined timeout of {:?}: {:?}",
                peer_addr, connection_timeout, timeout_error
            );
            BlockProbeResult::TCPFailure // this counts as network brokenness, should not count as BeyondUseless, just regular Bad
        }
        Ok(connection_might_have_failed) => {
            match connection_might_have_failed {
                Err(connection_failure) => {
                    let error_classification = classify_zebra_network_errors(&connection_failure);

                    match error_classification {
                        ErrorFlavor::Ephemeral(msg) => {
                            println!(
                                "Hash test connection with {:?} got an ephemeral error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::MustRetry
                        }
                        ErrorFlavor::Network(msg) => {
                            println!(
                                "Hash test connection with {:?} got an network error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::TCPFailure
                        }
                        ErrorFlavor::Protocol(msg) => {
                            println!(
                                "Hash test connection with {:?} got an protocol error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::ProtocolBad
                        }
                    }
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
                            // so far the only errors we've seen here look like one of these, and it's fine to ascribe them to BlockRequestFail and not to ProtocolBad
                            // SharedPeerError(ConnectionClosed)
                            // SharedPeerError(ConnectionReceiveTimeout)
                            // SharedPeerError(NotFoundResponse([Block(block::Hash("")), Block(block::Hash(""))]))
                            return BlockProbeResult::BlockRequestFail(peer_derived_data);
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
                                        return BlockProbeResult::BlockRequestOK(peer_derived_data);
                                    }
                                    // node returned a Blocks response, but it wasn't complete/correct for some reason
                                    BlockProbeResult::BlockRequestFail(peer_derived_data)
                                } // Response::Blocks(block_vector)
                                _ => {
                                    // connection established but we didn't get a Blocks response
                                    BlockProbeResult::BlockRequestFail(peer_derived_data)
                                }
                            } // match hash_query_protocol_response
                        } // Ok(hash_query_protocol_response)
                    } // match hash_query_response
                } // Ok(good_connection)
            }
        }
    }
}



async fn probe_for_peers_two(
    peer_addr: SocketAddr,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
    semaphore: Arc<Semaphore>,
) -> (SocketAddr, ProbeResult) {
    sleep(random_delay).await;
    let _permit = semaphore.acquire_owned().await.unwrap();
    println!(
        "Starting peer probe connection: peer addr is {:?}",
        peer_addr
    );
    let connection = connect_isolated_tcp_direct(
        network,
        peer_addr,
        String::from("/Seeder-and-feeder:0.0.0-alpha0/"),
    );
    let connection = timeout(timeouts.peers_timeout, connection);
    let connection = connection.await;

    match connection {
        Err(timeout_error) => {
            println!(
                "Probe connection with {:?} TIMED OUT: {:?}",
                peer_addr, timeout_error
            );
            return (peer_addr, ProbeResult::PeersFail);
        }
        Ok(connection_might_have_failed) => match connection_might_have_failed {
            Err(connection_failure) => {
                let error_classification = classify_zebra_network_errors(&connection_failure);

                match error_classification {
                    ErrorFlavor::Ephemeral(msg) => {
                        println!(
                            "Peer probe connection with {:?} got an ephemeral error: {:?}",
                            peer_addr, msg
                        );
                        (peer_addr, ProbeResult::MustRetryPeersResult)
                    }
                    ErrorFlavor::Network(msg) => {
                        println!(
                            "Peer probe connection with {:?} got an network error: {:?}",
                            peer_addr, msg
                        );
                        (peer_addr, ProbeResult::PeersFail)
                    }
                    ErrorFlavor::Protocol(msg) => {
                        println!(
                            "Peer probe connection with {:?} got an protocol error: {:?}",
                            peer_addr, msg
                        );
                        (peer_addr, ProbeResult::PeersFail)
                    }
                }
            }
            Ok(mut good_connection) => {
                for _attempt in 0..2 {
                    let peers_query_response = good_connection.call(Request::Peers).await;
                    if let Ok(zebra_network::Response::Peers(ref candidate_peers)) =
                        peers_query_response
                    {
                        if candidate_peers.len() > 1 {
                            return (
                                peer_addr,
                                ProbeResult::PeersResult(candidate_peers.to_vec()),
                            );
                        }
                    }
                }
                return (peer_addr, ProbeResult::PeersFail);
            }
        },
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

#[derive(Debug, Clone)]
enum BlockProbeResult {
    // Error on our host (too many FDs, etc), retryable and says nothing about the host or the network
    MustRetry, // increment nothing

    // Error establishing the TCP connection. Counts as a normal failure against the node
    TCPFailure, // increment total_attempts

    // Protocol negotiation failure. We were able to establish a TCP connection, but they said something zebra-network didn't like
    ProtocolBad, // increment total_attempts and tcp_connections_ok

    // Abnormal reply to the block request. We negotiated the protocol OK, but their reply to the block requests wasn't perfect
    BlockRequestFail(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotations_ok

    // Nominal reply to the block request
    BlockRequestOK(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotations_ok and valid_block_reply_ok
}

#[derive(Debug, Clone)]
struct PeerStats {
    total_attempts: u64,
    tcp_connections_ok: u64,
    protocol_negotiations_ok: u64,
    valid_block_reply_ok: u64,
    ewma_pack: EWMAPack,
    last_polled: Option<SystemTime>,
    last_protocol_negotiation: Option<SystemTime>,
    last_block_success: Option<SystemTime>,

    peer_derived_data: Option<PeerDerivedData>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PeerClassification {
    Unknown,               // We got told about this node but haven't yet queried it
    BeyondUseless, // We established a TCP connection, but protocol negotation has never worked. This probably isn't a Zcash or Zebra node.
    GenericBad, // We were able to establish a TCP connection, and the host is bad for a reason that doesn't make it BeyondUseless
    EventuallyMaybeSynced, // This looks like it could be a good node once it's synced enough, so poll it more often so it graduates earlier
    MerelySyncedEnough, // In the past 2 hours, this node served us a recent-enough block (synced-enough to the zcash chain) but doesn't meet uptime criteria
    AllGood, // Node meets all the legacy criteria (including uptime), can serve a recent-enough block
}

fn ancillary_checks_all_good(
    peer_derived_data: &PeerDerivedData,
    peer_address: &SocketAddr,
    peer_stats: &PeerStats,
    network: Network,
) -> PeerClassification {
    if !peer_derived_data
        .peer_services
        .intersects(PeerServices::NODE_NETWORK)
        || peer_derived_data.numeric_version < required_serving_version(network)
        || peer_derived_data.peer_height < required_height(network)
    {
        println!("Classifying node {:?} as GenericBad despite meeting other AllGood criteria. PeerStats: {:?}", peer_address, peer_stats);
        return PeerClassification::GenericBad;
    } else {
        return PeerClassification::AllGood;
    }
}

fn ancillary_checks_merely_synced(
    peer_derived_data: &PeerDerivedData,
    peer_address: &SocketAddr,
    peer_stats: &PeerStats,
    network: Network,
) -> PeerClassification {
    if !peer_derived_data
        .peer_services
        .intersects(PeerServices::NODE_NETWORK)
        || peer_derived_data.numeric_version < required_serving_version(network)
        || peer_derived_data.peer_height < required_height(network)
    {
        println!("Classifying node {:?} as GenericBad despite meeting other MerelySyncedEnough criteria. PeerStats: {:?}", peer_address, peer_stats);
        return PeerClassification::GenericBad;
    } else {
        return PeerClassification::MerelySyncedEnough;
    }
}

fn ancillary_checks_eventually_maybe_synced(
    peer_derived_data: &PeerDerivedData,
    peer_address: &SocketAddr,
    peer_stats: &PeerStats,
    network: Network,
) -> PeerClassification {
    if !peer_derived_data
        .peer_services
        .intersects(PeerServices::NODE_NETWORK)
        || peer_derived_data.numeric_version < required_serving_version(network)
    {
        println!("Classifying node {:?} as GenericBad despite meeting other EventuallyMaybeSynced criteria. PeerStats: {:?}", peer_address, peer_stats);
        return PeerClassification::GenericBad;
    } else {
        return PeerClassification::EventuallyMaybeSynced;
    }
}

// long term goal: add config options that look like:
// all_good_tests = numeric_version | peer_height
// merely_synced_enough_tests = numeric_version | peer_height
// to allow operator to customize tests should they become tetchy (also if we want to be extra we could disable the hash test to revert the seeder to the legacy behavior)

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
        // we never attempted to connect to this peer
        return PeerClassification::Unknown;
    }

    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        // negotiating the protocol at least once rules out Unknown and BeyondUseless
        // therefore, the node is one of {AllGood, MerelySyncedEnough, EventuallyMaybeSynced, GenericBad}
        // we test in the order of decreasing stringency, and return whenever we meet all the criteria for a condition

        // for AllGood, we need to pass the uptime criteria and (peer_services test, numeric version test, peer_height test)
        // for MerelySyncedEnough, we merely need a good block test in the past 2 hours and (peer_services test, numeric version test, peer_height test)
        // for EventuallyMaybeSynced, we need to have passed (peer_services test, numeric version test) in 24 hours, but peer_height test can fail
        // otherwise, we give a GenericBad

        // AllGood test section
        let ewmas = peer_stats.ewma_pack;
        if peer_stats.total_attempts <= 3
            && peer_stats.valid_block_reply_ok * 2 >= peer_stats.total_attempts
        {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }
        if ewmas.stat_2_hours.reliability > 0.85 && ewmas.stat_2_hours.count > 2.0 {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }
        if ewmas.stat_8_hours.reliability > 0.70 && ewmas.stat_8_hours.count > 4.0 {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }
        if ewmas.stat_1day.reliability > 0.55 && ewmas.stat_1day.count > 8.0 {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }
        if ewmas.stat_1week.reliability > 0.45 && ewmas.stat_1week.count > 16.0 {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }
        if ewmas.stat_1month.reliability > 0.35 && ewmas.stat_1month.count > 32.0 {
            return ancillary_checks_all_good(peer_derived_data, peer_address, peer_stats, network);
        }

        // MerelySyncedEnough test section
        // if it doesn't meet the uptime criteria but it passed the blocks test in the past 2 hours, serve it as an alternate
        if let Some(last_block_success) = peer_stats.last_block_success {
            if let Ok(duration) = last_block_success.elapsed() {
                if duration <= Duration::from_secs(60 * 60 * 2) {
                    return ancillary_checks_merely_synced(
                        peer_derived_data,
                        peer_address,
                        peer_stats,
                        network,
                    );
                }
            }
        }

        // EventuallyMaybeSynced test section
        // if last protocol negotiation was more than 24 hours ago, this is not worth special attention, keep polling it at the slower rate
        if let Some(last_protocol_negotiation) = peer_stats.last_protocol_negotiation {
            if let Ok(duration) = last_protocol_negotiation.elapsed() {
                if duration <= Duration::from_secs(60 * 60 * 24) {
                    return ancillary_checks_eventually_maybe_synced(
                        peer_derived_data,
                        peer_address,
                        peer_stats,
                        network,
                    );
                }
            }
        }

        // GenericBad test section
        println!("WARNING: classifying node {:?} with PeerStats {:?} as GenericBad despite having negotiated wire protocol: {:?}", peer_address, peer_stats, peer_derived_data);
        return PeerClassification::GenericBad;
    } else {
        // never were able to negotiate the wire protocol
        if peer_stats.tcp_connections_ok > 10 {
            // at least 10 TCP connections succeeded, but never been able to negotiate the Zcash protocol
            // this isn't a zcash node and isn't going to turn into one any time soon
            return PeerClassification::BeyondUseless;
        } else {
            // need more samples before hitting it with the worst possible penalty
            return PeerClassification::GenericBad;
        }
    }
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
    assert!(!sample_age.is_zero());
    update_ewma(&mut prev.stat_2_hours, sample_age, sample);
    update_ewma(&mut prev.stat_8_hours, sample_age, sample);
    update_ewma(&mut prev.stat_1day, sample_age, sample);
    update_ewma(&mut prev.stat_1week, sample_age, sample);
    update_ewma(&mut prev.stat_1month, sample_age, sample);
}

// (a single successful protocol negotiation -- ever -- gets you out of BeyondUseless:
// this in itself should be a bit less lenient, probably make it fade after one week or something)


// also we should have a way to detect how long it's been since a peer has been reported by a node,
// if it goes over a big threshold (like a month), stop tracking it altogether and remove it from the tracker


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
                    Duration::from_secs(60*8),    // base duration: 8 minutes
                    16,                           // number of attempts in exponential warmup
                    Duration::from_secs(60*60*16) // final duration: 16 hours
                );
            println!(
                "node {:?} is BeyondUseless, we try it again in {:?}",
                peer_address, threshold);
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
        PeerClassification::GenericBad => {
            let threshold = exponential_acquisition_threshold_secs(
                    peer_stats.as_ref().unwrap(),
                    Duration::from_secs(60*4),   // base duration: 4 minutes
                    16,                          // number of attempts in exponential warmup
                    Duration::from_secs(60*60*4) // final duration: 4 hours
                );
            println!(
                "node {:?} is GenericBad, we try it again in {:?}",
                peer_address, threshold);
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
        PeerClassification::EventuallyMaybeSynced => {
            let threshold = exponential_acquisition_threshold_secs(
                    peer_stats.as_ref().unwrap(),
                    Duration::from_secs(60*2), // base duration: 2 minutes
                    16,                        // number of attempts in exponential warmup
                    Duration::from_secs(60*30) // final duration: 30 minutes
                );
            println!(
                "node {:?} is EventuallyMaybeSynced, we try it again in {:?}",
                peer_address, threshold);
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)
        }
        PeerClassification::MerelySyncedEnough => {
            let threshold = exponential_acquisition_threshold_secs(
                    peer_stats.as_ref().unwrap(),
                    Duration::from_secs(60*2), // base duration: 2 minutes
                    16,                        // number of attempts in exponential warmup
                    Duration::from_secs(60*30) // final duration: 30 minutes
                );
            println!(
                "node {:?} is MerelySyncedEnough, we try it again in {:?}",
                peer_address, threshold);
            peer_last_polled_comparison(peer_stats.as_ref().unwrap(), threshold)        
        }
        PeerClassification::AllGood => {
            let threshold = exponential_acquisition_threshold_secs(
                    peer_stats.as_ref().unwrap(),
                    Duration::from_secs(60*2), // base duration: 2 minutes
                    16,                        // number of attempts in exponential warmup
                    Duration::from_secs(60*30) // final duration: 30 minutes
                );
            println!(
                "node {:?} is AllGood, we try it again in {:?}",
                peer_address, threshold);
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

fn exponential_acquisition_threshold_secs(peer_stats: &PeerStats, base_duration: Duration, attempts_cap: u64, final_duration: Duration) -> Duration {
    if peer_stats.total_attempts < attempts_cap {
        Duration::from_secs(peer_stats.total_attempts * base_duration.as_secs())
    } else {
        final_duration
    }
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
        "162.19.139.183:8235", "51.210.208.202:8836", "221.223.25.99:2331", "85.15.179.171:8233", "195.201.111.115:8233", "94.156.174.100:8233", "8.209.65.101:8233", "23.88.71.118:8233", "51.77.64.51:8233", "8.210.14.154:8233", "124.126.140.196:2331", "47.242.8.170:8233", "8.214.158.97:8233", "85.214.219.243:8233", "54.238.23.140:8233", "51.81.184.90:30834", "116.202.53.174:8533", "88.80.148.28:8233", "203.96.179.202:8233", "84.75.28.247:8233", "39.97.172.77:8233", "51.79.57.29:8233", "51.77.64.59:8233", "178.128.101.61:8233", "51.77.64.61:8233", "194.135.81.61:8233", "46.4.50.226:8233", "54.145.30.137:8233", "64.201.122.142:54324", "34.202.104.227:30570", "39.97.233.19:8233", "142.132.212.130:8836", "104.233.147.162:8233", "51.81.154.19:30834", "51.178.76.73:8836", "144.217.11.155:8233", "13.231.190.41:8233", "18.235.47.206:8233", "51.222.254.36:8233", "65.108.41.222:20005", "54.84.155.205:8233", "46.249.236.211:8233", "162.19.139.183:8233", "173.212.197.63:8233", "176.34.40.41:8233", "5.196.80.197:8233", "91.206.16.214:8233", "79.43.86.64:8233", "157.90.88.178:9058", "50.7.29.20:8233", "23.129.64.30:8233", "162.55.103.190:8233", "47.254.69.198:8233", "116.62.229.19:8233", "51.210.220.135:8836", "51.81.184.89:30834", "51.79.229.21:8233", "104.207.139.34:8233", "37.59.32.10:8233", "47.90.209.31:8233", "51.210.216.77:8836", "3.252.40.246:5001", "18.217.102.40:8233", "15.235.85.30:8233", "139.99.123.157:8233", "159.89.26.105:8233", "65.108.41.222:21005", "51.195.62.151:8233", "51.79.230.103:8233", "8.210.73.119:8233", "157.245.172.190:8233", "3.72.134.66:8233", "51.210.208.201:8836", "162.19.139.181:8233", "141.95.45.187:30834", "161.97.155.203:8233", "35.91.16.78:8233", "51.178.76.85:8836", "73.172.228.152:8233", "142.132.202.124:8836", "52.28.203.21:8233", "120.24.79.67:8233", "20.47.97.70:8233", "65.108.220.35:8233", "47.89.158.145:8233", "47.75.194.174:8233", "18.189.228.115:8233", "116.203.188.195:8233", "157.90.89.105:9058", "116.202.170.226:8233", "8.218.11.43:8233", "51.210.216.76:8836", "51.195.234.88:2838", "178.234.34.18:8233", "165.232.125.107:8233", "136.243.145.143:8233", "135.181.18.180:8233", "95.217.78.170:8233", "88.198.48.91:8233", "97.119.97.142:8233", "39.97.242.143:8233", "123.114.100.178:2331", "8.219.76.216:8233", "5.2.75.10:8233", "91.199.137.99:8233", "5.9.74.158:8233", "35.233.224.178:8233", "78.46.46.252:8233", "34.255.6.39:5001", "162.19.139.182:8233", "3.16.30.39:8233", "47.242.184.215:8233", "51.210.114.183:8836", "47.252.44.174:8233", "35.230.70.77:8233", "135.181.79.230:8233", "65.21.137.242:8233", "47.254.176.240:8233", "37.187.88.208:8233", "168.119.147.118:8233", "47.243.196.68:8233", "162.19.136.65:30834", "34.196.173.50:8233", "8.218.10.114:8233", "62.210.69.194:8233", "209.141.47.197:8233", "44.197.66.202:8233", "35.72.109.227:8233", "44.200.177.58:8233", "51.222.245.186:8233", "111.90.145.162:8233", "135.148.55.16:8233", "51.195.63.10:30834", "65.21.40.28:8233", "46.4.192.189:8233", "78.189.206.225:8233", "8.209.80.185:8233", "147.135.11.134:8233"];

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

fn update_serving_nodes(
    serving_nodes_shared: &Arc<RwLock<ServingNodes>>,
    internal_peer_tracker: &HashMap<SocketAddr, Option<PeerStats>>,
) {
    let mut primary_nodes = HashSet::new();
    let mut alternate_nodes = HashSet::new();

    for (key, value) in internal_peer_tracker {
        if key.ip().is_global() {
            let classification = get_classification(&value, &key, Network::Mainnet);
            if classification == PeerClassification::AllGood {
                primary_nodes.insert(key.clone());
            }
            if classification == PeerClassification::MerelySyncedEnough {
                alternate_nodes.insert(key.clone());
            }
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
    if new_peer_address.ip().is_global() {
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
}

struct Timeouts {
    hash_timeout: Duration,
    peers_timeout: Duration,
}
#[derive(Debug, Clone)]
enum ProbeResult {
    HashResult(PeerStats),
    MustRetryHashResult,
    PeersResult(Vec<MetaAddr>),
    PeersFail,
    MustRetryPeersResult,
}
async fn hash_probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
    semaphore: Arc<Semaphore>,
) -> (SocketAddr, ProbeResult) {
    // we always return the SockAddr of the server we probed, so we can reissue queries
    let mut new_peer_stats = match old_stats {
        None => PeerStats {
            total_attempts: 0,
            tcp_connections_ok: 0,
            protocol_negotiations_ok: 0,
            valid_block_reply_ok: 0,
            ewma_pack: EWMAPack::default(),
            last_polled: None,
            last_protocol_negotiation: None,
            last_block_success: None,
            peer_derived_data: None,
        },
        Some(old_stats) => old_stats.clone(),
    };
    sleep(random_delay).await;
    let permit = semaphore.acquire_owned().await.unwrap();
    let current_poll_time = SystemTime::now(); // sample time here, in case peer req takes a while
    let poll_res = hash_probe_inner(proband_address, network, timeouts.hash_timeout).await;
    drop(permit);
    //println!("result = {:?}", poll_res);
    match poll_res {
        BlockProbeResult::MustRetry => {
            println!("Retry the connection!");
            return (proband_address, ProbeResult::MustRetryHashResult);
        }
        BlockProbeResult::TCPFailure => {
            new_peer_stats.total_attempts += 1;
            new_peer_stats.tcp_connections_ok += 0;
            new_peer_stats.protocol_negotiations_ok += 0;
            new_peer_stats.valid_block_reply_ok += 0;

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                false,
            );
            new_peer_stats.last_polled = Some(current_poll_time);
        }
        BlockProbeResult::ProtocolBad => {
            new_peer_stats.total_attempts += 1;
            new_peer_stats.tcp_connections_ok += 1;
            new_peer_stats.protocol_negotiations_ok += 0;
            new_peer_stats.valid_block_reply_ok += 0;

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                false,
            );
            new_peer_stats.last_polled = Some(current_poll_time);

        }
        BlockProbeResult::BlockRequestFail(new_peer_data) => {
            new_peer_stats.total_attempts += 1;
            new_peer_stats.tcp_connections_ok += 1;
            new_peer_stats.protocol_negotiations_ok += 1;
            new_peer_stats.valid_block_reply_ok += 0;


            new_peer_stats.peer_derived_data = Some(new_peer_data);

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                false,
            );
            new_peer_stats.last_protocol_negotiation = Some(current_poll_time);
            new_peer_stats.last_polled = Some(current_poll_time);

        }
        BlockProbeResult::BlockRequestOK(new_peer_data) => {
            new_peer_stats.total_attempts += 1;
            new_peer_stats.tcp_connections_ok += 1;
            new_peer_stats.protocol_negotiations_ok += 1;
            new_peer_stats.valid_block_reply_ok += 1;

            new_peer_stats.peer_derived_data = Some(new_peer_data);
            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.last_polled,
                current_poll_time,
                true,
            );
            new_peer_stats.last_polled = Some(current_poll_time);
            new_peer_stats.last_protocol_negotiation = Some(current_poll_time);
            new_peer_stats.last_block_success = Some(current_poll_time);

            println!("new ewma pack is {:?}",new_peer_stats.ewma_pack)
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (proband_address, ProbeResult::HashResult(new_peer_stats));
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
