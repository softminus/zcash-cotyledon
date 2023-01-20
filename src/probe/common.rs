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
use zebra_network::{connect_isolated_tcp_direct, HandshakeError, Request, Version};

use crate::probe::{ProbeResult, Timeouts};

#[derive(Debug, Clone, PartialEq)]
pub struct PeerDerivedData {
    pub numeric_version: Version,
    pub peer_services: PeerServices,
    pub peer_height: Height,
    pub user_agent: String,
    pub relay: bool,
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
pub enum ErrorFlavor {
    Network(String),
    Ephemeral(String),
    Protocol(String),
}

pub fn classify_zebra_network_errors(
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

pub async fn peer_probe(
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
