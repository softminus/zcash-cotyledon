#[derive(Debug, Clone)]
enum ProbeResult {
    Result(PeerStats),
    MustRetryProbe,
    PeersResult(Vec<MetaAddr>),
    PeersFail,
    MustRetryPeers,
}

enum ProbeType {
    Block,
    Headers,
    Negotation
}

async fn hash_probe_and_update(
    proband_address: SocketAddr,
    old_stats: Option<PeerStats>,
    network: Network,
    timeouts: &Timeouts,
    random_delay: Duration,
    semaphore: Arc<Semaphore>,
    probe_type: ProbeType
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
            return (proband_address, ProbeResult::MustRetryHashResult);
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

            println!("new ewma pack is {:?}",new_peer_stats.ewma_pack)
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (proband_address, ProbeResult::HashResult(new_peer_stats));
}




async fn hash_probe_and_update(
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
            return (proband_address, ProbeResult::MustRetryHashResult);
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

            println!("new ewma pack is {:?}",new_peer_stats.ewma_pack)
        }
    }
    new_peer_stats.last_polled = Some(current_poll_time);
    return (proband_address, ProbeResult::HashResult(new_peer_stats));
}
