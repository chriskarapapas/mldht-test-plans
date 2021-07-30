module github.com/mmlab-aueb/mldht-test-plans/tests

go 1.16

require (
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ipfs-util v0.0.2
	github.com/libp2p/go-libp2p v0.14.0
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-kad-dht v0.12.1
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/multiformats/go-multiaddr-net v0.2.0
	github.com/testground/sdk-go v0.2.7
)

replace github.com/libp2p/go-libp2p-kad-dht => github.com/chriskarapapas/dhtold v0.1.5
