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
use chrono::Utc;
use tokio::sync::oneshot::error::TryRecvError;


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

pub struct InboundSetupData {
    /// A shared list of peer addresses.
    pub address_book: Arc<std::sync::Mutex<AddressBook>>,
}

/// Tracks the internal state of the [`Inbound`] service during setup.
pub enum Setup {
    /// Waiting for service setup to complete.
    ///
    /// All requests are ignored.
    Pending {
        /// A oneshot channel used to receive required services,
        /// after they are set up.
        setup: oneshot::Receiver<InboundSetupData>,
    },

    /// Setup is complete.
    ///
    /// All requests are answered.
    Initialized {
        /// A shared list of peer addresses.
        address_book: Arc<std::sync::Mutex<zn::AddressBook>>,
    },

    /// Temporary state used in the inbound service's internal initialization code.
    ///
    /// If this state occurs outside the service initialization code, the service panics.
    FailedInit,

    /// Setup failed, because the setup channel permanently failed.
    /// The service keeps returning readiness errors for every request.
    FailedRecv {
        /// The original channel error.
        error: SharedRecvError,
    },
}

/// A wrapper around `Arc<TryRecvError>` that implements `Error`.
#[derive(thiserror::Error, Debug, Clone)]
#[error(transparent)]
pub struct SharedRecvError(Arc<TryRecvError>);

impl From<TryRecvError> for SharedRecvError {
    fn from(source: TryRecvError) -> Self {
        Self(Arc::new(source))
    }
}





pub struct Inbound {
    /// Provides service dependencies, if they are available.
    ///
    /// Some services are unavailable until Zebra has completed setup.
    setup: Setup,
}

impl Inbound {
    /// Create a new inbound service.
    ///
    /// Dependent services are sent via the `setup` channel after initialization.
    pub fn new(setup: oneshot::Receiver<InboundSetupData>) -> Inbound {
        Inbound {
            setup: Setup::Pending { setup },
        }
    }

    /// Remove `self.setup`, temporarily replacing it with an invalid state.
    fn take_setup(&mut self) -> Setup {
        let mut setup = Setup::FailedInit;
        std::mem::swap(&mut self.setup, &mut setup);
        setup
    }
}



impl Service<zn::Request> for Inbound {
    type Response = zn::Response;
    type Error = zn::BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check whether the setup is finished, but don't wait for it to
        // become ready before reporting readiness. We expect to get it "soon",
        // and reporting unreadiness might cause unwanted load-shedding, since
        // the load-shed middleware is unable to distinguish being unready due
        // to load from being unready while waiting on setup.

        // Every setup variant handler must provide a result
        let result;

        self.setup = match self.take_setup() {
            Setup::Pending { mut setup } => match setup.try_recv() {
                Ok(setup_data) => {
                    let InboundSetupData {
                        address_book,
                    } = setup_data;

                    result = Ok(());
                    Setup::Initialized {
                        address_book
                    }
                }
                Err(TryRecvError::Empty) => {
                    // There's no setup data yet, so keep waiting for it
                    result = Ok(());
                    Setup::Pending { setup }
                }
                Err(error @ TryRecvError::Closed) => {
                    // Mark the service as failed, because setup failed
                    let error: SharedRecvError = error.into();
                    result = Err(error.clone().into());
                    Setup::FailedRecv { error }
                }
            },
            // Make sure previous setups were left in a valid state
            Setup::FailedInit => unreachable!("incomplete previous Inbound initialization"),
            // If setup failed, report service failure
            Setup::FailedRecv { error } => {
                result = Err(error.clone().into());
                Setup::FailedRecv { error }
            }
            // Clean up completed download tasks, ignoring their results
            Setup::Initialized {
                address_book,
            } => {
                result = Ok(());
                Setup::Initialized {
                    address_book,
                }
            }
        };

        // Make sure we're leaving the setup in a valid state
        if matches!(self.setup, Setup::FailedInit) {
            unreachable!("incomplete Inbound initialization after poll_ready state handling");
        }

        // TODO:
        //  * do we want to propagate backpressure from the download queue or its outbound network?
        //    currently, the download queue waits for the outbound network in the download future,
        //    and drops new requests after it reaches a hard-coded limit. This is the
        //    "load shed directly" pattern from #1618.
        //  * currently, the state service is always ready, unless its buffer is full.
        //    So we might also want to propagate backpressure from its buffer.
        //  * poll_ready needs to be implemented carefully, to avoid hangs or deadlocks.
        //    See #1593 for details.
        Poll::Ready(result)
    }

    /// Call the inbound service.
    ///
    /// Errors indicate that the peer has done something wrong or unexpected,
    /// and will cause callers to disconnect from the remote peer.
    fn call(&mut self, req: zn::Request) -> Self::Future {
        let (address_book) = match &mut self.setup {
            Setup::Initialized {
                address_book,
            } => (address_book),
            _ => {
                return async { Ok(zn::Response::Nil) }.boxed();
            }
        };

        match req {
            zn::Request::Peers => {
                // # Security
                //
                // We truncate the list to not reveal our entire peer set in one call.
                // But we don't monitor repeated requests and the results are shuffled,
                // a crawler could just send repeated queries and get the full list.
                //
                // # Correctness
                //
                // Briefly hold the address book threaded mutex while
                // cloning the address book. Then sanitize in the future,
                // after releasing the lock.
                let peers = address_book.lock().unwrap().clone();
                println!("OUTISDE THE ASYNC BLOCK; SENDING SOME PEERS! {:?}", peers);

                async move {
                    // Correctness: get the current time after acquiring the address book lock.
                    //
                    // This time is used to filter outdated peers, so it doesn't really matter
                    // if we get it when the future is created, or when it starts running.
                    let now = Utc::now();

                    // Send a sanitized response
                    let mut peers = peers.sanitized(now);

                    // Truncate the list
                    //
                    // TODO: replace with div_ceil once it stabilises
                    //       https://github.com/rust-lang/rust/issues/88581
                    println!("SENDING SOME PEERS! {:?}", peers);
                    if peers.is_empty() {
                        // We don't know if the peer response will be empty until we've sanitized them.
                        Ok(zn::Response::Nil)
                    } else {
                        Ok(zn::Response::Peers(peers))
                    }
                }.boxed()
            }

            _ => {
                println!("we don't handle request types of {}", req);
                return async { Ok(zn::Response::Nil) }.boxed();
            }
            zn::Request::Ping(_) => {
                unreachable!("ping requests are handled internally");
            }
        }
    }
}


#[tokio::main]
async fn main()
{
    let config = Config::default();
    //let nil_inbound_service = |x| async move {println!("{}", x); Ok(Response::Nil) };

    let (setup_tx, setup_rx) = oneshot::channel();
    let inbound = ServiceBuilder::new().buffer(10).service(Inbound::new(setup_rx));

    let (peer_set, address_book) = init(config, inbound, NoChainTip).await;
    //println!("egg {}", std::any::type_name_of_val(&peer_set));

    let setup_data = InboundSetupData { address_book};

    setup_tx.send(setup_data);
    loop {
        std::thread::sleep(Duration::new(1,0));
    }

}