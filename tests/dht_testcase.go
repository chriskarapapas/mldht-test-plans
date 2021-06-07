package main

import (
	"context"
	"net"
	"fmt"
	"time"
	//"sync"
	//"math/rand"

	"github.com/testground/sdk-go/runtime"
	"github.com/testground/sdk-go/sync"
	"github.com/testground/sdk-go/network"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-core/crypto"
	tptu "github.com/libp2p/go-libp2p-transport-upgrader"
	tcp "github.com/libp2p/go-tcp-transport"
	"github.com/ipfs/go-datastore"
)

type NodeInfo struct
{
	Addr *peer.AddrInfo //<- Be careful, variable name must start with capital
}
var node_info_topic = sync.NewTopic("nodeinfo", &NodeInfo{})

func getSubnetAddr(runenv *runtime.RunEnv) (*net.TCPAddr, error) {
	log.SetAllLoggers(log.LevelWarn)
	subnet := runenv.TestSubnet
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok {
			if subnet.Contains(ip.IP) {
				tcpAddr := &net.TCPAddr{IP: ip.IP}
				return tcpAddr, nil
			}
		} else {
			panic(fmt.Sprintf("%T", addr))
		}
	}
	return nil, fmt.Errorf("no network interface found. Addrs: %v", addrs)
}

func DHTTest(runenv *runtime.RunEnv) error {
	runenv.RecordMessage("Starting test case")
	ctx    := context.Background()
	synClient := sync.MustBoundClient(ctx, runenv)
	//var kademliaDHT *dht.IpfsDHT
	defer synClient.Close()

	libp2pInitialized      := sync.State("libp2p-init-completed")
	nodeBootstrapCompleted := sync.State("bootstrap-completed")
	dhtBootstrapCompleted  := sync.State("bootstrap-completed")
	experimentCompleted    := sync.State("experiment-completed")
	totalNodes             := runenv.TestInstanceCount

	// instantiate a network client; see 'Traffic shaping' in the docs.
	netClient := network.NewClient(synClient, runenv)
	runenv.RecordMessage("waiting for network initialization")
	netClient.MustWaitNetworkInitialized(ctx)
	
	/*
	Configure libp2p
	*/
	tcpAddr, err    := getSubnetAddr(runenv)
	mutliAddr, err  := manet.FromNetAddr(tcpAddr)
	priv, _, err := crypto.GenerateKeyPair(
		crypto.Ed25519, // Select your key type. Ed25519 are nice short
		-1,             // Select key length when possible (i.e. RSA).
	)
	libp2pNode, err := libp2p.New(ctx,
		libp2p.Identity(priv),
		
		libp2p.Transport(func(u *tptu.Upgrader) *tcp.TcpTransport {
			tpt := tcp.NewTCPTransport(u)
			tpt.DisableReuseport = true
			return tpt
		}),
		
		libp2p.EnableNATService(), 
		libp2p.ForceReachabilityPublic(),
		libp2p.ListenAddrs(mutliAddr),

	)
	//var ds datastore.Batching
	//ds = dssync.MutexWrap(datastore.NewMapDatastore())
	dhtOptions := []dht.Option{
		dht.ProtocolPrefix("/testground"),
		dht.Datastore(datastore.NewMapDatastore()),
		//dhtopts.BucketSize(opts.BucketSize),
		//dhtopts.RoutingTableRefreshQueryTimeout(opts.Timeout),
		//dhtopts.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()}),
	}
	
	kademliaDHT, err := dht.New(ctx, libp2pNode, dhtOptions...)
	if err!= nil {
		runenv.RecordMessage("Error in seting up Kadmlia %s", err)
	}
	
	addrInfo := host.InfoFromHost(libp2pNode)
	runenv.RecordMessage("libp2p initilization complete")
	seq := synClient.MustSignalAndWait(ctx, libp2pInitialized,totalNodes)
	
	/*
	Synchronize nodes
	*/
	if seq==1 { // I am the bootstrap node, publish
		synClient.Publish(ctx, node_info_topic,  &NodeInfo{addrInfo})
	}
	bootstap_info_channel := make(chan *NodeInfo)
	synClient.Subscribe(ctx, node_info_topic,  bootstap_info_channel)
	bootstrap_node := <-bootstap_info_channel
	runenv.RecordMessage("Received from channel %s", bootstrap_node.Addr)
	
	/*
	Bootstap nodes
	*/	
	
	if seq == 1 {//the first nodes is assumed to be the bootstrap node
		synClient.MustSignalEntry(ctx, nodeBootstrapCompleted)
	} else{
		//Bootstrap one by one
		<-synClient.MustBarrier(ctx, nodeBootstrapCompleted, int(seq-1)).C
		runenv.RecordMessage("Node %d will bootstrap from %s", seq, bootstrap_node.Addr)
		if err := kademliaDHT.Host().Connect(ctx, *bootstrap_node.Addr); err != nil {
			runenv.RecordMessage("Error in connecting %s", err)
		} else {
			runenv.RecordMessage("Connection established")
		}
		synClient.MustSignalEntry(ctx, nodeBootstrapCompleted)	
	}
	/*
	for _, addr := range dht.DefaultBootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		// We ignore errors as some bootstrap peers may be down

		kademliaDHT.Host().Connect(ctx, *pi)
		fmt.Println("Connected to bootstrap node", pi.ID)
	}*/
	synClient.MustSignalAndWait(ctx, dhtBootstrapCompleted,totalNodes)
	
	time.Sleep(time.Second * 20)
	//synClient.MustSignalAndWait(ctx, dhtBootstrapCompleted,totalNodes)
	runenv.RecordMessage("Routing table size %d", kademliaDHT.RoutingTable().Size())
	
	

	

	/*
	Create records and announce that you can provide them
	*/
	
	packet := fmt.Sprintf("Hello from %s", addrInfo)
	cid := cid.NewCidV0(u.Hash([]byte(packet)))

	err = kademliaDHT.Provide(ctx, cid, true)
	if err == nil {
		runenv.RecordMessage("Provided CID: %s", cid)
	}

	if err != nil {
		panic(err)
	}

	synClient.MustSignalAndWait(ctx, experimentCompleted,totalNodes)
	runenv.RecordMessage("Ending test case")
	return nil
}