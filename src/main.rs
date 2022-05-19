#![feature(type_name_of_val)]

use zebra_network::init;

use std::time::Duration;
use zebra_chain::{chain_tip::NoChainTip, parameters::Network};
use tokio::{pin, select, sync::oneshot};
use tokio::runtime::Runtime;

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
use tower::{builder::ServiceBuilder, buffer::Buffer, timeout::Timeout, util::BoxService, Service, ServiceExt, service_fn};

use zebra_network as zn;
use zebra_state as zs;
use zn::Response;
use zebra_chain::{
    block::{self, Block},
    transaction::UnminedTxId,
};
use zebra_network::{
    constants::{ADDR_RESPONSE_LIMIT_DENOMINATOR, MAX_ADDRS_IN_MESSAGE},
    AddressBook, InventoryResponse, Config
};


// async fn inbound_service (request) {
//     Ok(Response::Nil)
// }


fn main()
{
    let config = Config::default();
    let rt  = Runtime::new().unwrap();
    let nil_inbound_service = |x| async move { println!("{}", x); Ok(Response::Nil) };

    // let inbound = ServiceBuilder::new()
    //     .load_shed()
    //     .buffer(inbound::downloads::MAX_INBOUND_CONCURRENCY)
    //     .service(Inbound::new(setup_rx));
    let x = init(config, service_fn(nil_inbound_service), NoChainTip);
    //println!("{}", std::any::type_name_of_val(&nil_inbound_service));
    rt.block_on(x);
}