use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;

use zebra_chain::block::Height;
use zebra_chain::parameters::Network;

use zebra_network::types::PeerServices;
use zebra_network::Version;

use super::ewma::EWMAPack;
use crate::probe::common::{PeerDerivedData, REQUIRED_MAINNET_HEIGHT, REQUIRED_TESTNET_HEIGHT};
use crate::probe::{PeerClassification, ProbeType};

#[derive(Debug, Clone, Default)]
pub struct PeerStats {
    pub ewma_pack: EWMAPack,

    pub tcp_connection: ProbeStat,
    pub protocol_negotiation: ProbeStat,
    pub block_probe_valid: bool,
    pub block_probe: ProbeStat,

    pub peer_derived_data: Option<PeerDerivedData>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProbeStat {
    pub attempt_count: u64,
    pub last_polled: Option<SystemTime>,

    pub success_count: u64,
    pub last_success: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub enum GatingProbes {
    Negotiation(Duration),
    Block(Duration),
    BlockLenient(Duration),
    NumericVersion(Vec<Version>),
    UserAgent(Vec<String>),
    PeerHeight(Height),
    PeerServicesBitmap(PeerServices),
    RelayBit
}

pub struct ProbeConfiguration {
    all_good_gating_probes: Vec<GatingProbes>,
    merely_synced_gating_probes: Vec<GatingProbes>,
    eventually_maybe_gating_probes: Vec<GatingProbes>,
    merely_synced_timeout: Duration,
    eventually_synced_timeout: Duration,
    beyond_useless_count_threshold: u64,
    beyond_useless_age_threshold: Duration,
}

pub fn dns_servable(peer_address: SocketAddr, network: Network) -> bool {
    return peer_address.port() == network.default_port();
}

pub fn all_good_test(
    peer_stats: &PeerStats,
    _network: Network,
    probes_config: &ProbeConfiguration,
) -> Option<PeerClassification> {
    if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        // negotiating the protocol at least once rules out Unknown and BeyondUseless
        // therefore, the node is one of {AllGood, MerelySyncedEnough, EventuallyMaybeSynced, GenericBad}
        // we test in the order of decreasing stringency, and return whenever we meet all the criteria for a condition

        // for AllGood, we need to pass the uptime criteria and (peer_services test, numeric version test, peer_height test)
        // for MerelySyncedEnough, we merely need a good block test in the past 2 hours and (peer_services test, numeric version test, peer_height test)
        // for EventuallyMaybeSynced, we need to have passed (peer_services test, numeric version test) in 24 hours, but peer_height test can fail
        // otherwise, we give a GenericBad

        // AllGood test section
        let ewmas = peer_stats.ewma_pack;

        let probe_stat = peer_stats.protocol_negotiation;

        if probe_stat.attempt_count <= 3 && probe_stat.success_count * 2 >= probe_stat.attempt_count
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
    probes_config: &ProbeConfiguration,
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
        if check_gating(
            peer_stats,
            network,
            &probes_config.all_good_gating_probes,
        ) {
            return all_good;
        }
    }

    if check_gating(
        peer_stats,
        network,
        &probes_config.merely_synced_gating_probes,
    ) {
        return PeerClassification::MerelySyncedEnough;
    }


    if check_gating(
        peer_stats,
        network,
        &probes_config.eventually_maybe_gating_probes,
    ) {
        return PeerClassification::EventuallyMaybeSynced;
    }

    if let Some(beyond_useless) = beyond_useless_test(peer_stats, network, &probes_config) {
        return beyond_useless;
    }

    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        println!("WARNING: classifying node {:?} with PeerStats {:?} as GenericBad despite having negotiated wire protocol: {:?}", peer_address, peer_stats, peer_derived_data);
    }
    return PeerClassification::GenericBad;
}


pub fn beyond_useless_test(
    peer_stats: &PeerStats,
    _network: Network,
    probes_config: &ProbeConfiguration,
) -> Option<PeerClassification> {
    if peer_stats.tcp_connection.attempt_count > probes_config.beyond_useless_count_threshold { // only put nodes into BeyondUseless if we've tried enough times
        if peer_stats.protocol_negotiation.success_count == 0 { // and if we've never been able to negotiate the protocol with them
            return Some(PeerClassification::BeyondUseless);
        }

        if let Some(last_protocol_success) = peer_stats.protocol_negotiation.last_success { // or if the last successful protocol negotiation was a really long time ago
            if let Ok(duration) = last_protocol_success.elapsed() {
                if duration <= probes_config.beyond_useless_age_threshold {
                    return Some(PeerClassification::BeyondUseless);
                }
            }
        }
    }
    return None;
}

fn check_gating(
    peer_stats: &PeerStats,
    network: Network,
    gating_probes: &Vec<GatingProbes>,
) -> bool {
    for probe in gating_probes {
        if !match probe {
            GatingProbes::Block(timeout) => {
                gating_check_block(peer_stats, network, timeout)
            }
            GatingProbes::BlockLenient(timeout) => {
                gating_check_block_lenient(peer_stats, network, timeout)
            }
            GatingProbes::NumericVersion(valid_versions) => {
                gating_check_numeric_version(peer_stats, network, valid_versions)
            }
            GatingProbes::UserAgent(valid_user_agents) => {
                gating_check_user_agent(peer_stats, network, valid_user_agents)
            }
            GatingProbes::PeerHeight(required_height) => {
                gating_check_peer_height(peer_stats, network, required_height)
            }
            GatingProbes::Negotiation(timeout) => {
                gating_check_negotiation(peer_stats, network, timeout)
            }
            GatingProbes::PeerServicesBitmap(bitmap) => {
                gating_check_services_bitmap(peer_stats, network, bitmap)
            }
            GatingProbes::RelayBit => {
                gating_check_relay_bit(peer_stats, network)
            }
        } {
            return false;
        }
    }
    return true;
}

fn gating_check_block(
    peer_stats: &PeerStats,
    network: Network,
    timeout: &Duration,
) -> bool {
    if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        if let Some(last_block_success) = peer_stats.block_probe.last_success {
            if let Ok(duration) = last_block_success.elapsed() {
                if duration <= *timeout {
                    return peer_stats.block_probe_valid;
                }
            }
        }
    }
    return false;
}


fn gating_check_block_lenient(
    peer_stats: &PeerStats,
    network: Network,
    timeout: &Duration,
) -> bool {
    if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        if let Some(last_block_success) = peer_stats.block_probe.last_success {
            if let Ok(duration) = last_block_success.elapsed() {
                if duration <= *timeout {
                    return true;
                }
            }
        }
    }
    return false;
}

fn gating_check_numeric_version(
    peer_stats: &PeerStats,
    network: Network,
    valid_versions: &Vec<Version>,
) -> bool {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        for valid_ver in valid_versions {
            if peer_derived_data.numeric_version == *valid_ver {
                return true
            }
        }
    }
    return false;
}

fn gating_check_user_agent(
    peer_stats: &PeerStats,
    network: Network,
    valid_user_agents: &Vec<String>,
) -> bool {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        for valid_ua in valid_user_agents {
            if peer_derived_data.user_agent == *valid_ua {
                return true
            }
        }
    }
    return false;
}
fn gating_check_negotiation(
    peer_stats: &PeerStats,
    network: Network,
    timeout: &Duration,
) -> bool {
    if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        if let Some(last_protocol_negotiation) = peer_stats.protocol_negotiation.last_success {
            if let Ok(duration) = last_protocol_negotiation.elapsed() {
                if duration <= *timeout {
                    return true;
                }
            }
        }
    }
    return false;
}


fn gating_check_peer_height(
    peer_stats: &PeerStats,
    network: Network,
    required_height: &Height
) -> bool {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        if peer_derived_data.peer_height >= *required_height {
            return true;
        }
    }
    return false;
}

fn gating_check_services_bitmap(
    peer_stats: &PeerStats,
    network: Network,
    bitmap: &PeerServices
) -> bool {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        return peer_derived_data.peer_services.intersects(*bitmap);
    }
    return false;
}


fn gating_check_relay_bit(
    peer_stats: &PeerStats,
    network: Network
) -> bool {
    if let Some(peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
        return peer_derived_data.relay;
    }
    return false;
}

// pub fn merely_synced_test(
//     peer_stats: &PeerStats,
//     _network: Network,
//     probes_config: &ProbeConfiguration,
// ) -> Option<PeerClassification> {
//     if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
//         // MerelySyncedEnough test section
//         // if it doesn't meet the uptime criteria but it passed the blocks test in the past 2 hours, serve it as an alternate
//         if let Some(last_block_success) = peer_stats.block_probe.last_success {
//             if let Ok(duration) = last_block_success.elapsed() {
//                 if duration <= probes_config.merely_synced_timeout {
//                     return Some(PeerClassification::MerelySyncedEnough);
//                 }
//             }
//         }
//     }
//     return None;
// }

// pub fn eventually_maybe_test(
//     peer_stats: &PeerStats,
//     _network: Network,
//     probes_config: &ProbeConfiguration,
// ) -> Option<PeerClassification> {
//     if let Some(_peer_derived_data) = peer_stats.peer_derived_data.as_ref() {
//         // EventuallyMaybeSynced test section
//         // if last protocol negotiation was more than 24 hours ago, this is not worth special attention, keep polling it at the slower rate
//         if let Some(last_protocol_negotiation) = peer_stats.protocol_negotiation.last_success {
//             if let Ok(duration) = last_protocol_negotiation.elapsed() {
//                 if duration <= probes_config.eventually_synced_timeout {
//                     return Some(PeerClassification::EventuallyMaybeSynced);
//                 }
//             }
//         }
//     }
//     return None;
// }


// fn ancillary_checks_all_good(
//     peer_derived_data: &PeerDerivedData,
//     peer_address: &SocketAddr,
//     peer_stats: &PeerStats,
//     network: Network,
// ) -> PeerClassification {
//     if !peer_derived_data
//         .peer_services
//         .intersects(PeerServices::NODE_NETWORK)
//         || peer_derived_data.numeric_version < required_serving_version(network)
//         || peer_derived_data.peer_height < required_height(network)
//     {
//         println!("Classifying node {:?} as GenericBad despite meeting other AllGood criteria. PeerStats: {:?}", peer_address, peer_stats);
//         return PeerClassification::GenericBad;
//     } else {
//         return PeerClassification::AllGood;
//     }
// }

// fn ancillary_checks_merely_synced(
//     peer_derived_data: &PeerDerivedData,
//     peer_address: &SocketAddr,
//     peer_stats: &PeerStats,
//     network: Network,
// ) -> PeerClassification {
//     if !peer_derived_data
//         .peer_services
//         .intersects(PeerServices::NODE_NETWORK)
//         || peer_derived_data.numeric_version < required_serving_version(network)
//         || peer_derived_data.peer_height < required_height(network)
//     {
//         println!("Classifying node {:?} as GenericBad despite meeting other MerelySyncedEnough criteria. PeerStats: {:?}", peer_address, peer_stats);
//         return PeerClassification::GenericBad;
//     } else {
//         return PeerClassification::MerelySyncedEnough;
//     }
// }

// fn ancillary_checks_eventually_maybe_synced(
//     peer_derived_data: &PeerDerivedData,
//     peer_address: &SocketAddr,
//     peer_stats: &PeerStats,
//     network: Network,
// ) -> PeerClassification {
//     if !peer_derived_data
//         .peer_services
//         .intersects(PeerServices::NODE_NETWORK)
//         || peer_derived_data.numeric_version < required_serving_version(network)
//     {
//         println!("Classifying node {:?} as GenericBad despite meeting other EventuallyMaybeSynced criteria. PeerStats: {:?}", peer_address, peer_stats);
//         return PeerClassification::GenericBad;
//     } else {
//         return PeerClassification::EventuallyMaybeSynced;
//     }
// }

// fn required_height(network: Network) -> Height {
//     match network {
//         Network::Mainnet => *REQUIRED_MAINNET_HEIGHT,
//         Network::Testnet => *REQUIRED_TESTNET_HEIGHT,
//     }
// }

// fn required_serving_version(network: Network) -> Version {
//     match network {
//         Network::Mainnet => Version(170_100),
//         Network::Testnet => Version(170_040),
//     }
// }

