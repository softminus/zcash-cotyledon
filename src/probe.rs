pub mod block_probe;
pub mod classify;
pub mod common;
pub mod ewma;
pub mod interval_logic;
pub mod negotiation_probe;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::Semaphore;
use tokio::time::sleep;

use zebra_chain::parameters::Network;
use zebra_network::types::MetaAddr;

use crate::probe::block_probe::{block_probe_inner, BlockProbeResult};
use crate::probe::classify::{ProbeStat, PeerStats};
use crate::probe::ewma::{probe_stat_update, update_ewma_pack};
use crate::probe::negotiation_probe::{negotiation_probe_inner, NegotiationProbeResult};

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProbeType {
    Block,
    Negotiation,
}

async fn probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
    semaphore: Arc<Semaphore>,
    probe_user_agent: String,
    probe_type: ProbeType,
) -> (SocketAddr, ProbeResult) {
    // we always return the SockAddr of the server we probed, so we can reissue queries
    let mut new_peer_stats: PeerStats = match old_stats {
        None => Default::default(),
        Some(old_stats) => old_stats.clone(),
    };
    sleep(random_delay).await;
    let permit = semaphore.acquire_owned().await.unwrap();
    let current_poll_time = SystemTime::now(); // sample time here, in case peer req takes a while
    let probe_result_generic = match probe_type {
        ProbeType::Block => {
            let probe_res =
                block_probe_inner(proband_address, network, timeouts.hash_timeout, probe_user_agent).await;
            drop(permit);
            block_probe_update(probe_res, current_poll_time, &mut new_peer_stats)
        }
        ProbeType::Negotiation => {
            let probe_res =
                negotiation_probe_inner(proband_address, network, timeouts.hash_timeout, probe_user_agent).await;
            drop(permit);
            negotiation_probe_update(probe_res, current_poll_time, &mut new_peer_stats)
        }
    };
    return (proband_address, probe_result_generic);
}

fn block_probe_update(
    probe_res: BlockProbeResult,
    current_poll_time: SystemTime,
    new_peer_stats: &mut PeerStats,
) -> ProbeResult {
    match probe_res {
        BlockProbeResult::MustRetry => {
            // no updating any stats, the probe was invalid
            println!("Retry the connection!");
            return ProbeResult::MustRetryProbe;
        }
        BlockProbeResult::TCPFailure => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, false, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                false,
                current_poll_time,
            );
            probe_stat_update(&mut new_peer_stats.block_probe, false, current_poll_time);
            new_peer_stats.block_probe_valid = true;
            return ProbeResult::Result(new_peer_stats.clone());
        }
        BlockProbeResult::ProtocolBad => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, true, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                false,
                current_poll_time,
            );
            probe_stat_update(&mut new_peer_stats.block_probe, false, current_poll_time);
            new_peer_stats.block_probe_valid = true;

            return ProbeResult::Result(new_peer_stats.clone());
        }
        BlockProbeResult::BlockRequestFail(new_peer_data) => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, true, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                true,
                current_poll_time,
            );
            probe_stat_update(&mut new_peer_stats.block_probe, false, current_poll_time);
            new_peer_stats.block_probe_valid = true;

            new_peer_stats.peer_derived_data = Some(new_peer_data);
            return ProbeResult::Result(new_peer_stats.clone());
        }
        BlockProbeResult::BlockRequestOK(new_peer_data) => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, true, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                true,
                current_poll_time,
            );
            probe_stat_update(&mut new_peer_stats.block_probe, true, current_poll_time);
            new_peer_stats.block_probe_valid = true;

            new_peer_stats.peer_derived_data = Some(new_peer_data);
            return ProbeResult::Result(new_peer_stats.clone());
        }
    }
}

fn negotiation_probe_update(
    probe_res: NegotiationProbeResult,
    current_poll_time: SystemTime,
    new_peer_stats: &mut PeerStats,
) -> ProbeResult {
    match probe_res {
        NegotiationProbeResult::MustRetry => {
            println!("Retry the connection!");
            return ProbeResult::MustRetryProbe;
        }
        NegotiationProbeResult::TCPFailure => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, false, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                false,
                current_poll_time,
            );

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.protocol_negotiation.last_polled,
                current_poll_time,
                false,
            );
            return ProbeResult::Result(new_peer_stats.clone());
        }
        NegotiationProbeResult::ProtocolBad => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, true, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                false,
                current_poll_time,
            );

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.protocol_negotiation.last_polled,
                current_poll_time,
                false,
            );
            return ProbeResult::Result(new_peer_stats.clone());
        }
        NegotiationProbeResult::ProtocolOK(new_peer_data) => {
            probe_stat_update(&mut new_peer_stats.tcp_connection, true, current_poll_time);
            probe_stat_update(
                &mut new_peer_stats.protocol_negotiation,
                true,
                current_poll_time,
            );

            update_ewma_pack(
                &mut new_peer_stats.ewma_pack,
                new_peer_stats.protocol_negotiation.last_polled,
                current_poll_time,
                true,
            );
            if let Some(old_peer_data) = &new_peer_stats.peer_derived_data {
                if *old_peer_data != new_peer_data {
                    println!("Invalidating block probe since peer metadata changed from {:?} to {:?}", old_peer_data, new_peer_data );
                    new_peer_stats.block_probe_valid = false;
                }
            }
            new_peer_stats.peer_derived_data = Some(new_peer_data);
            return ProbeResult::Result(new_peer_stats.clone());
        }
    }
}
