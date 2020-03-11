package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"

	etcd "github.com/coreos/etcd/clientv3"
)

type EtcdPeerDiscoverer struct {
	cli    *etcd.Client
	ctx    context.Context
	wg     *sync.WaitGroup
	prefix string

	peers map[string]struct{}
}

func NewEtcdPeerDiscoverer(ctx context.Context, etcdCli *etcd.Client) *EtcdPeerDiscoverer {
	peerd := &EtcdPeerDiscoverer{
		cli:    etcdCli,
		ctx:    ctx,
		prefix: "prefix",
		wg:     &sync.WaitGroup{},
		peers:  make(map[string]struct{}),
	}

	peerd.wg.Add(1)

	go peerd.watchChanges()

	return peerd
}

func (e *EtcdPeerDiscoverer) Register(addr string) error {

	peerKey := e.prefix + addr

	lease, err := e.cli.Grant(e.ctx, 10)
	if err != nil {
		log.Fatal(err)
	}

	_, err = e.cli.Put(e.ctx, peerKey, addr, etcd.WithLease(lease.ID))
	if err != nil {
		log.Fatal(err)
	}

	ka, err := e.cli.KeepAlive(e.ctx, lease.ID)
	if err != nil {
		return err
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			// keepalive was lost
			if ka == nil {
				fmt.Println("we lost ka")
				return
			}

			select {
			case _, ok := <-ka:
				if !ok {
					fmt.Println("ka not ok")
					return
				}
			case <-e.ctx.Done():
				if _, err := e.cli.Delete(e.ctx, peerKey); err != nil {
					fmt.Println(err)
				}
				fmt.Println("deleted peer ", peerKey)

				if _, err := e.cli.Revoke(e.ctx, lease.ID); err != nil {
					fmt.Println(err)
				}
				fmt.Println("revoked lease ", lease.ID)

				return
			}
		}
	}()

	return nil
}

func (e *EtcdPeerDiscoverer) GetPeers() []string {
	peers := make([]string, len(e.peers))
	for k := range e.peers {
		peers = append(peers, k)
	}
	return peers
}

func (e *EtcdPeerDiscoverer) Close() error {
	e.wg.Wait()
	return nil
}

func (e *EtcdPeerDiscoverer) watchChanges() {
	defer e.wg.Done()

	e.collectPeers()

	watchCh := e.cli.Watch(e.ctx, e.prefix, etcd.WithPrefix(), etcd.WithPrevKV())
	for {
		select {
		case ch, ok := <-watchCh:
			if !ok {
				return
			}

			for _, ev := range ch.Events {
				fmt.Println(ev.Kv)

				switch ev.Type {
				case etcd.EventTypePut:
					if ev.Kv != nil {
						fmt.Println("added peer ", string(ev.Kv.Value))
						e.peers[string(ev.Kv.Value)] = struct{}{}
					}
				case etcd.EventTypeDelete:
					if ev.PrevKv != nil {
						fmt.Println("removed peer ", string(ev.PrevKv.Value))
						delete(e.peers, string(ev.PrevKv.Value))
					}
				}
			}
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *EtcdPeerDiscoverer) collectPeers() error {
	r, err := e.cli.Get(e.ctx, e.prefix, etcd.WithPrefix())
	if err != nil {
		return err
	}

	for _, v := range r.Kvs {
		e.peers[string(v.Value)] = struct{}{}
	}

	return nil
}
