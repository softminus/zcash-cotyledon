use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;

use zebra_chain::block::Height;
use zebra_chain::parameters::Network;

use zebra_network::types::PeerServices;
use zebra_network::Version;

use crate::probe::internal::{PeerDerivedData, REQUIRED_MAINNET_HEIGHT, REQUIRED_TESTNET_HEIGHT};
use crate::probe::PeerClassification;
use crate::probe::classify::ProbeStat;


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

pub fn probe_stat_update(probe_stat: &mut ProbeStat, new_data_point: bool, new_poll_time: SystemTime) {
    probe_stat.attempt_count += 1;
    probe_stat.last_polled   = Some(new_poll_time);
    if new_data_point {
        probe_stat.success_count += 1;
        probe_stat.last_success = Some(new_poll_time);
    }
}
