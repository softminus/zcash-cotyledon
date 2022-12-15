use crate::probe::ProbeResult;


use futures_util::StreamExt;



use std::collections::{HashSet};
use std::net::{SocketAddr};


use std::sync::{Arc, LazyLock};
use std::time::{Duration};

use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};


use tower::Service;
use zebra_chain::block::{Hash, Height};
use zebra_chain::parameters::Network;
use zebra_chain::serialization::SerializationError;
use zebra_consensus::CheckpointList;
use zebra_network::types::{PeerServices};
use zebra_network::{
    connect_isolated_tcp_direct, HandshakeError, InventoryResponse, Request, Response, Version,
};

use crate::probe::Timeouts;

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


#[derive(Debug, Clone)]
pub enum NegotiationProbeResult {
    // Error on our host (too many FDs, etc), retryable and says nothing about the host or the network
    MustRetry, // increment nothing

    // Error establishing the TCP connection. Counts as a normal failure against the node
    TCPFailure, // increment total_attempts

    // Protocol negotiation failure. We were able to establish a TCP connection, but they said something zebra-network didn't like
    ProtocolBad, // increment total_attempts and tcp_connections_ok

    // Protocol negotiation went OK
    ProtocolOK(PeerDerivedData), // increment total_attempts and tcp_connections_ok and protocol_negotiations_ok
}


#[derive(Debug, Clone)]
pub struct PeerDerivedData {
    pub numeric_version: Version,
    pub peer_services: PeerServices,
    pub peer_height: Height,
    pub _user_agent: String,
    pub _relay: bool,
}

pub static HASH_CHECKPOINTS_MAINNET: LazyLock<HashSet<Hash>> = LazyLock::new(|| {
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

pub static REQUIRED_MAINNET_HEIGHT: LazyLock<Height> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Mainnet);
    checkpoint.max_height()
});

pub static REQUIRED_TESTNET_HEIGHT: LazyLock<Height> = LazyLock::new(|| {
    let checkpoint = CheckpointList::new(Network::Testnet);
    checkpoint.max_height()
});

pub static HASH_CHECKPOINTS_TESTNET: LazyLock<HashSet<Hash>> = LazyLock::new(|| {
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



pub async fn hash_probe_inner(
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





async fn headers_probe_inner(
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
                    let headers_request = Request::FindHeaders {known_blocks: headers_vec, stop: None};
                    let headers_query_response = good_connection
                        .call(headers_request.clone())
                        .await;
                    //println!("headers query response is {:?}", headers_query_response);
                    match headers_query_response {
                        Err(protocol_error) => {
                            println!("protocol failure after requesting headers with peer {}: {:?}", peer_addr, protocol_error);
                            // so far the only errors we've seen here look like one of these, and it's fine to ascribe them to BlockRequestFail and not to ProtocolBad
                            // SharedPeerError(ConnectionClosed)
                            // SharedPeerError(ConnectionReceiveTimeout)
                            // SharedPeerError(NotFoundResponse([Block(block::Hash("")), Block(block::Hash(""))]))
                            return HeadersProbeResult::HeadersFail(peer_derived_data);
                        }
                        Ok(headers_query_protocol_response) => {
                            println!("Headers request gave us {:?}", headers_query_protocol_response);
                            return HeadersProbeResult::HeadersOK(peer_derived_data);
                        }
                    }
                }
            }
        }
    }
}



pub async fn probe_for_peers_two(
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
                        (peer_addr, ProbeResult::MustRetryPeers)
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



