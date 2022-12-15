
#[derive(Clone, Debug)]
pub struct DnsContext {
    serving_nodes_shared: Arc<RwLock<ServingNodes>>,
    serving_network: Network,
}

#[async_trait::async_trait]
impl dns::RequestHandler for DnsContext {
    async fn handle_request<R: dns::ResponseHandler>(
        &self,
        request: &dns::Request,
        response_handle: R,
    ) -> dns::ResponseInfo {
        match self.do_handle_request(request, response_handle).await {
            Some(response_info) => response_info,
            None => {
                println!("Failed to respond to query: {:?}", request.query());
                let mut header = dnsop::Header::new();
                header.set_response_code(dnsop::ResponseCode::ServFail);
                header.into()
            }
        }
    }
}

impl DnsContext {
    async fn do_handle_request<R: dns::ResponseHandler>(
        &self,
        request: &dns::Request,
        mut response_handle: R,
    ) -> Option<dns::ResponseInfo> {
        if request.op_code() != dnsop::OpCode::Query {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        if request.message_type() != dnsop::MessageType::Query {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        if request.query().query_class() != dnsrr::DNSClass::IN {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::ServFail),
                    )
                    .await
                    .unwrap(),
            );
        }
        let endpoint = dnsrr::LowerName::from(
            dnsrr::Name::from_str("mainnet-test-seed.electriccoin.co").unwrap(),
        ); // make me configurable
        if *request.query().name() != endpoint {
            let response = MessageResponseBuilder::from_message_request(request);
            return Some(
                response_handle
                    .send_response(
                        response.error_msg(request.header(), dnsop::ResponseCode::NXDomain),
                    )
                    .await
                    .unwrap(),
            );
        }

        let builder = MessageResponseBuilder::from_message_request(request);
        let mut header = dnsop::Header::response_from_request(request.header());
        header.set_authoritative(true);
        let mut records = Vec::new();
        {
            let serving_nodes = self.serving_nodes_shared.read().unwrap();
            for peer in serving_nodes
                .primaries
                .iter()
                .chain(serving_nodes.alternates.iter())
            {
                if dns_servable(*peer, self.serving_network) {
                    match request.query().query_type() {
                        dnsrr::RecordType::A => {
                            if let IpAddr::V4(ipv4) = peer.ip() {
                                records.push(dnsrr::Record::from_rdata(
                                    request.query().name().into(),
                                    60,
                                    dnsrr::RData::A(ipv4),
                                ))
                            }
                        }
                        dnsrr::RecordType::AAAA => {
                            if let IpAddr::V6(ipv6) = peer.ip() {
                                records.push(dnsrr::Record::from_rdata(
                                    request.query().name().into(),
                                    60,
                                    dnsrr::RData::AAAA(ipv6),
                                ))
                            }
                        }
                        _ => {} // if the query is something other than A or AAAA, we'll have no records in the reply, and that means a NODATA
                    }
                }
            }
        }

        let response = builder.build(header, records.iter(), &[], &[], &[]);
        Some(response_handle.send_response(response).await.unwrap())
    }
}

