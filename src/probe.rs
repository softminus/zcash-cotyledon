pub mod classify;
pub mod internal;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::Semaphore;
use tokio::time::sleep;

use zebra_chain::parameters::Network;
use zebra_network::types::MetaAddr;

use internal::BlockProbeResult;
use crate::probe::classify::PeerStats;
use crate::probe::classify::{update_ewma_pack, EWMAPack};
use crate::probe::internal::hash_probe_inner;

pub struct Timeouts {
    pub hash_timeout: Duration,
    pub peers_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerClassification {
    Unknown,               // We got told about this node but haven't yet queried it
    BeyondUseless, // We established a TCP connection, but protocol negotation has never worked. This probably isn't a Zcash or Zebra node.
    GenericBad, // We were able to establish a TCP connection, and the host is bad for a reason that doesn't make it BeyondUseless
    EventuallyMaybeSynced, // This looks like it could be a good node once it's synced enough, so poll it more often so it graduates earlier
    MerelySyncedEnough, // In the past 2 hours, this node served us a recent-enough block (synced-enough to the zcash chain) but doesn't meet uptime criteria
    AllGood, // Node meets all the legacy criteria (including uptime), can serve a recent-enough block
}

#[derive(Debug, Clone)]
pub enum ProbeResult {
    Result(PeerStats),
    MustRetryProbe,
    PeersResult(Vec<MetaAddr>),
    PeersFail,
    MustRetryPeers,
}

pub enum ProbeType {
    Block,
    Headers,
    Negotation,
}

async fn ewma_probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
    semaphore: Arc<Semaphore>,
    _probe_type: ProbeType,
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
            return (proband_address, ProbeResult::MustRetryProbe);
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

            println!("new ewma pack is {:?}", new_peer_stats.ewma_pack)
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (proband_address, ProbeResult::Result(new_peer_stats));
}

pub async fn hash_probe_and_update(
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
            return (proband_address, ProbeResult::MustRetryProbe);
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

            println!("new ewma pack is {:?}", new_peer_stats.ewma_pack)
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (proband_address, ProbeResult::Result(new_peer_stats));
}
