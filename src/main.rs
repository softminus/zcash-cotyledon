use zebra_network::init;

use std::time::Duration;
use zebra_chain::{chain_tip::NoChainTip, parameters::Network};
use tokio::{pin, select, sync::oneshot};
use tower::{builder::ServiceBuilder, util::BoxService};


use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    future::{FutureExt, TryFutureExt},
    stream::Stream,
};
use tower::{buffer::Buffer, timeout::Timeout, util::BoxService, Service, ServiceExt};

use zebra_network as zn;
use zebra_state as zs;

use zebra_chain::{
    block::{self, Block},
    transaction::UnminedTxId,
};
use zebra_network::{
    constants::{ADDR_RESPONSE_LIMIT_DENOMINATOR, MAX_ADDRS_IN_MESSAGE},
    AddressBook, InventoryResponse,
};



fn main()
{

    let unused_v4 = "0.0.0.0:0".parse().unwrap();

    let config = Config {
        initial_mainnet_peers: peers,
        peerset_initial_target_size: PEERSET_INITIAL_TARGET_SIZE,

        network: Network::Mainnet,
        listen_addr: unused_v4,

        ..Config::default()
    };


    // let inbound = ServiceBuilder::new()
    //     .load_shed()
    //     .buffer(inbound::downloads::MAX_INBOUND_CONCURRENCY)
    //     .service(Inbound::new(setup_rx));
    init(config, NoChainTip);
}