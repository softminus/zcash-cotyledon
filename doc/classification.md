# Peer probe

At calculated intervals (depending on the current classification of the node and the current state of the seeder), the "peer probe" is sent to each node. A connection is established with `zebra-network`, and a [Peers](https://doc-internal.zebra.zfnd.org/zebra_network/protocol/internal/request/enum.Request.html#variant.Peers) request is sent. Due to internal `zcashd` considerations (rate limiting and protocol weirdness), not all the peer probes are successful, but that's ok, since we try them often enough. This probe type is used solely to enable crawling the network, and does not affect a peer's classification at all.

# Uptime probe

At calculated intervals (depending on the current classification of the node and the current state of the seeder), the "uptime probe" is sent to each node. The uptime probe is simply a protocol negotiation probe: a connection is established with `zebra-network`. If the protocol is successfully negotiated, it is deemed as having passed.

This probe has the advantage of being extremely light on bandwidth, which is good, since it's the probe that gets used most often. Also, we can use this to detect the protocol number / version string changing, which signals to us that the remote node changed the software it runs (and therefore we should invalidate the result of the more resource-intensive probes -- the "gating" probes).

# Gating probes

The uptime probe is good for detecting if the remote node is down, but not good enough to detect if a node is synced or if it even is an actual zcash node. For this, we have a set of user-configurable "gating" probes must also pass in order to validate that a remote node can actually be served to clients.

## Block probe

A connection is established with `zebra-network` and a request is made with `BlocksByHash` for multiple block hashes (determined at compile time as the last two `zebra` checkpoints). If the queried node returns a block which hashes to the initial query, it is determined to have passed. This probe has the advantage of being highly accurate at discriminating a real zcashd node from a chainfork, but it has the disadvantage of provoking high network usage (since the entire block is sent for each probe).

If this probe is enabled in the "gating" set, it must have succeeded within a user-configurable (default: 2 hours) interval for the node to be served. Should the node's version number / user-agent string change, a new block probe must succeed before the node can be served again.

## Numeric version number test

The advertised numeric version must be equal to (or exceed) the configured numeric version.

## User agent string test

The user agent string must match one of the the user-configured user agent strings.

## Peer height test

The advertised peer height must be greater than or equal to the user-configured height.


# Salvage logic
If a node has been able to pass the block probe within a user-configurable time window (default: 2 hours), the node is placed in the `MerelySyncedEnough` state and is served alongside the nodes which do pass the more stringent tests.

This is so a failure in the logic or probing mechanisms (or a network failure) does not totally destroy the seeder's functionality.

# Prospective logic

If a node meets the numeric version number test and has a user agent string which matches the user agent string test, it's placed in the `EventuallyMaybeSynced` state, which allows it to be polled more often so it graduates to servable earlier.