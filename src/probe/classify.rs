use crate::probe::PeerClassification;

use futures_util::StreamExt;

use std::net::SocketAddr;

use std::time::{Duration, SystemTime};

use zebra_chain::block::Height;
use zebra_chain::parameters::Network;

use zebra_network::types::PeerServices;
use zebra_network::Version;

use crate::probe::internal::PeerDerivedData;

use crate::probe::internal::{REQUIRED_MAINNET_HEIGHT, REQUIRED_TESTNET_HEIGHT};

#[derive(Debug, Clone)]
pub struct PeerStats {
    pub total_attempts: u64,
    pub tcp_connections_ok: u64,
    pub protocol_negotiations_ok: u64,
    pub valid_block_reply_ok: u64,
    pub ewma_pack: EWMAPack,
    pub last_polled: Option<SystemTime>,
    pub last_protocol_negotiation: Option<SystemTime>,
    pub last_block_success: Option<SystemTime>,

    pub peer_derived_data: Option<PeerDerivedData>,
}

#[derive(Debug, Clone, Copy)]
pub struct EWMAPack {
    stat_2_hours: EWMAState,
    stat_8_hours: EWMAState,
    stat_1day: EWMAState,
    stat_1week: EWMAState,
    stat_1month: EWMAState,
}

#[derive(Debug, Clone, Copy, Default)]
struct EWMAState {
    scale: Duration,
    weight: f64,
    count: f64,
    reliability: f64,
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

pub fn get_classification(
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

pub fn update_ewma_pack(
    prev: &mut EWMAPack,
    previous_polling_time: Option<SystemTime>,
    current_polling_time: SystemTime,
    sample: bool,
) {
    let mut sample_age = Duration::from_secs(1); // default weighting, in case we haven't polled it yet

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
