use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures_util::StreamExt;

use tower::Service;

use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};

use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_chain::serialization::SerializationError;
use zebra_consensus::CheckpointList;
use zebra_network::types::PeerServices;
use zebra_network::{
    connect_isolated_tcp_direct, HandshakeError, InventoryResponse, Request, Response, Version,
};

use crate::probe::{ProbeResult, Timeouts};
use crate::probe::common::{ErrorFlavor, classify_zebra_network_errors, PeerDerivedData};
use crate::probe::common::{HASH_CHECKPOINTS_MAINNET, HASH_CHECKPOINTS_TESTNET};

#[derive(Debug, Clone)]
pub enum HeadersProbeResult {
    // Error on our host (too many FDs, etc), retryable and says nothing about the host or the network
    MustRetry, // increment nothing

    // Error establishing the TCP connection. Counts as a normal failure against the node
    TCPFailure, // increment total_attempts

    // Protocol negotiation failure. We were able to establish a TCP connection, but they said something zebra-network didn't like
    ProtocolBad, // increment total_attempts and tcp_connections_ok

    // Abnormal reply to the headers request. We negotiated the protocol OK, but their reply to the block requests wasn't perfect
    HeadersFail(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotiations_ok

    // Nominal reply to the headers request
    HeadersOK(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotiations_ok and valid_headers_reply_ok
}




pub(super) async fn headers_probe_inner(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> HeadersProbeResult {
    println!(
        "Starting new headers probe connection: peer addr is {:?}",
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
    let headers_vec = Vec::from_iter(hash_checkpoints);
    println!("headers vec {:?}", headers_vec);

    match connection {
        Err(timeout_error) => {
            println!(
                "Headers test connection with {:?} failed due to user-defined timeout of {:?}: {:?}",
                peer_addr, connection_timeout, timeout_error
            );
            HeadersProbeResult::TCPFailure // this counts as network brokenness, should not count as BeyondUseless, just regular Bad
        }
        Ok(connection_might_have_failed) => {
            match connection_might_have_failed {
                Err(connection_failure) => {
                    let error_classification = classify_zebra_network_errors(&connection_failure);

                    match error_classification {
                        ErrorFlavor::Ephemeral(msg) => {
                            println!(
                                "Headers test connection with {:?} got an ephemeral error: {:?}",
                                peer_addr, msg
                            );
                            HeadersProbeResult::MustRetry
                        }
                        ErrorFlavor::Network(msg) => {
                            println!(
                                "Headers test connection with {:?} got an network error: {:?}",
                                peer_addr, msg
                            );
                            HeadersProbeResult::TCPFailure
                        }
                        ErrorFlavor::Protocol(msg) => {
                            println!(
                                "Headers test connection with {:?} got an protocol error: {:?}",
                                peer_addr, msg
                            );
                            HeadersProbeResult::ProtocolBad
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
                    let headers_request = Request::FindHeaders {
                        known_blocks: headers_vec,
                        stop: None,
                    };
                    let headers_query_response =
                        good_connection.call(headers_request.clone()).await;
                    //println!("headers query response is {:?}", headers_query_response);
                    match headers_query_response {
                        Err(protocol_error) => {
                            println!(
                                "protocol failure after requesting headers with peer {}: {:?}",
                                peer_addr, protocol_error
                            );
                            // so far the only errors we've seen here look like one of these, and it's fine to ascribe them to BlockRequestFail and not to ProtocolBad
                            // SharedPeerError(ConnectionClosed)
                            // SharedPeerError(ConnectionReceiveTimeout)
                            // SharedPeerError(NotFoundResponse([Block(block::Hash("")), Block(block::Hash(""))]))
                            return HeadersProbeResult::HeadersFail(peer_derived_data);
                        }
                        Ok(headers_query_protocol_response) => {
                            println!(
                                "Headers request gave us {:?}",
                                headers_query_protocol_response
                            );
                            return HeadersProbeResult::HeadersOK(peer_derived_data);
                        }
                    }
                }
            }
        }
    }
}

