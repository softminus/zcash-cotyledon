use std::collections::HashSet;
use std::net::SocketAddr;

use std::time::Duration;

use futures_util::StreamExt;

use tower::Service;

use tokio::time::timeout;

use zebra_chain::parameters::Network;

use zebra_network::{connect_isolated_tcp_direct, InventoryResponse, Request, Response};

use crate::probe::common::{
    classify_zebra_network_errors, ErrorFlavor, PeerDerivedData, HASH_CHECKPOINTS_MAINNET,
    HASH_CHECKPOINTS_TESTNET,
};

#[derive(Debug, Clone)]
pub enum BlockProbeResult {
    // Error on our host (too many FDs, etc), retryable and says nothing about the host or the network
    MustRetry, // increment nothing

    // Error establishing the TCP connection. Counts as a normal failure against the node
    TCPFailure, // increment total_attempts

    // Protocol negotiation failure. We were able to establish a TCP connection, but they said something zebra-network didn't like
    ProtocolBad, // increment total_attempts and tcp_connections_ok

    // Abnormal reply to the block request. We negotiated the protocol OK, but their reply to the block requests wasn't perfect
    BlockRequestFail(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotiations_ok

    // Nominal reply to the block request
    BlockRequestOK(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotiations_ok and valid_block_reply_ok
}

pub(super) async fn block_probe_inner(
    peer_addr: SocketAddr,
    network: Network,
    connection_timeout: Duration,
) -> BlockProbeResult {
    println!(
        "Starting new block probe connection: peer addr is {:?}",
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
                "Block test connection with {:?} failed due to user-defined timeout of {:?}: {:?}",
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
                                "Block test connection with {:?} got an ephemeral error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::MustRetry
                        }
                        ErrorFlavor::Network(msg) => {
                            println!(
                                "Block test connection with {:?} got an network error: {:?}",
                                peer_addr, msg
                            );
                            BlockProbeResult::TCPFailure
                        }
                        ErrorFlavor::Protocol(msg) => {
                            println!(
                                "Block test connection with {:?} got an protocol error: {:?}",
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
                    let user_agent = good_connection.connection_info.remote.user_agent.clone();
                    let relay = good_connection.connection_info.remote.relay;
                    let peer_derived_data = PeerDerivedData {
                        numeric_version,
                        peer_services,
                        peer_height,
                        user_agent,
                        relay,
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
