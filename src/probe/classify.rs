use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;

use zebra_chain::block::Height;
use zebra_chain::parameters::Network;

use zebra_network::types::PeerServices;
use zebra_network::Version;

use crate::probe::common::{PeerDerivedData, REQUIRED_MAINNET_HEIGHT, REQUIRED_TESTNET_HEIGHT};
use crate::probe::{PeerClassification, ProbeType};
use super::ewma::EWMAPack;

#[derive(Debug, Clone, Default)]
pub struct PeerStats {
    pub ewma_pack: EWMAPack,

    pub tcp_connection: ProbeStat,
    pub protocol_negotiation: ProbeStat,
    pub block_probe: ProbeStat,
    pub header_probe: ProbeStat,

    pub peer_derived_data: Option<PeerDerivedData>,
}


#[derive(Debug, Clone, Default)]
pub struct ProbeStat {
    pub attempt_count: u64,
    pub last_polled:  Option<SystemTime>,

    pub success_count: u64,
    pub last_success: Option<SystemTime>,
}

pub enum AugmentedProbeTypes {
    Block,
    Headers,
    Negotiation,
    NumericVersion,
    UserAgent,
    PeerHeight
}


pub struct ProbeConfiguration {
    ewma_probe: ProbeType,
    all_good_gating_probes: Vec<AugmentedProbeTypes>,
    merely_synced_gating_probes: Vec<AugmentedProbeTypes>,
    eventually_maybe_gating_probes: Vec<AugmentedProbeTypes>,
    merely_synced_timeout: Duration,
    eventually_synced_timeout: Duration,
    beyond_useless_count_threshold: u64,
}


pub fn all_good_test(peer_stats: &PeerStats, network: Network, probes_config: &ProbeConfiguration) -> Option<PeerClassification> {
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

        let probe_stat = match probes_config.ewma_probe {
            ProbeType::Block => peer_stats.block_probe,
            ProbeType::Headers => peer_stats.header_probe,
            ProbeType::Negotiation => peer_stats.protocol_negotiation,
        };
        if probe_stat.attempt_count <= 3
            && probe_stat.success_count * 2 >= probe_stat.attempt_count
        {
            return Some(PeerClassification::AllGood);
        }
        if ewmas.stat_2_hours.reliability > 0.85 && ewmas.stat_2_hours.count > 2.0 {
            return Some(PeerClassification::AllGood);
        }
        if ewmas.stat_8_hours.reliability > 0.70 && ewmas.stat_8_hours.count > 4.0 {
            return Some(PeerClassification::AllGood);
        }
        if ewmas.stat_1day.reliability > 0.55 && ewmas.stat_1day.count > 8.0 {
            return Some(PeerClassification::AllGood);
        }
        if ewmas.stat_1week.reliability > 0.45 && ewmas.stat_1week.count > 16.0 {
            return Some(PeerClassification::AllGood);
        }
        if ewmas.stat_1month.reliability > 0.35 && ewmas.stat_1month.count > 32.0 {
            return Some(PeerClassification::AllGood);
        }
    }
    return None;
}


pub fn get_classification(
    peer_stats: &Option<PeerStats>,
    peer_address: &SocketAddr,
    network: Network,
    probes_config: &ProbeConfiguration
) -> PeerClassification {

    // generic logic, rule out never having attempted this peer
    let peer_stats = match peer_stats {
        None => return PeerClassification::Unknown,
        Some(peer_stats) => peer_stats,
    };

    // generic logic, rule out never having attempted this peer
    if peer_stats.tcp_connection.attempt_count == 0 {
        // we never attempted to connect to this peer
        return PeerClassification::Unknown;
    }

    if let Some(all_good) = all_good_test(peer_stats, network, &probes_config) {
        if check_gating(peer_stats, network, probes_config.all_good_gating_probes) {
            return all_good;
        }
    }

    if let Some(merely) = merely_synced_test(peer_stats, network, &probes_config) {
        if check_gating(peer_stats, network, probes_config.merely_synced_gating_probes) {
            return merely;
        }
    }

    if let Some(maybe) = eventually_maybe_test(peer_stats, network, &probes_config) {
        if check_gating(peer_stats, network, probes_config.eventually_maybe_gating_probes) {
            return maybe;
        }
    }

    if let Some(beyond_useless) = beyond_useless_test(peer_stats, network, &probes_config) {
        return beyond_useless;
    }

    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        println!("WARNING: classifying node {:?} with PeerStats {:?} as GenericBad despite having negotiated wire protocol: {:?}", peer_address, peer_stats, peer_derived_data);
    }
    return PeerClassification::GenericBad;
}


pub fn merely_synced_test(peer_stats: &PeerStats, network: Network, probes_config: &ProbeConfiguration) -> Option<PeerClassification> {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        // MerelySyncedEnough test section
        // if it doesn't meet the uptime criteria but it passed the blocks test in the past 2 hours, serve it as an alternate
        if let Some(last_block_success) = peer_stats.block_probe.last_success {
            if let Ok(duration) = last_block_success.elapsed() {
                if duration <= probes_config.merely_synced_timeout {
                    return Some(PeerClassification::MerelySyncedEnough);
                }
            }
        }
    }
    return None;
}

pub fn eventually_maybe_test(peer_stats: &PeerStats, network: Network, probes_config: &ProbeConfiguration) -> Option<PeerClassification> {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        // EventuallyMaybeSynced test section
        // if last protocol negotiation was more than 24 hours ago, this is not worth special attention, keep polling it at the slower rate
        if let Some(last_protocol_negotiation) = peer_stats.protocol_negotiation.last_success {
            if let Ok(duration) = last_protocol_negotiation.elapsed() {
                if duration <= probes_config.eventually_synced_timeout {
                    return Some(PeerClassification::EventuallyMaybeSynced);
                }
            }
        }
    }
    return None;
}

pub fn beyond_useless_test(peer_stats: &PeerStats, network: Network, probes_config: &ProbeConfiguration) -> Option<PeerClassification> {
    if peer_stats.tcp_connection.attempt_count > probes_config.beyond_useless_count_threshold
        && peer_stats.protocol_negotiation.success_count == 0 {
        return Some(PeerClassification::BeyondUseless);
    }
    return None;
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

pub fn dns_servable(peer_address: SocketAddr, network: Network) -> bool {
    return peer_address.port() == network.default_port();
}
