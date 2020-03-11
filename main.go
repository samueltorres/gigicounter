package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/oklog/run"
	"github.com/peterbourgon/ff/v3"
	"github.com/samueltorres/gigicounter/pkg/cluster"
	"go.uber.org/zap"
)

func main() {
	fs := flag.NewFlagSet("my-program", flag.ExitOnError)
	var (
		listenAddr = fs.String("listen-addr", "localhost:8080", "listen address")
		etcdAddr   = fs.String("etcd-addr", "localhost:2379", "")
		_          = fs.String("config", "", "config file (optional)")
	)

	ff.Parse(fs, os.Args[1:])

	ctx, cancel := context.WithCancel(context.Background())

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	fmt.Println("got here")

	etcdcli, err := etcd.New(etcd.Config{
		Endpoints:   []string{*etcdAddr},
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		logger.Sugar().Fatalf("error initializing etcd client: %v", err)
	}

	peerd := cluster.NewEtcdPeerDiscoverer(ctx, etcdcli)
	err = peerd.Register(*listenAddr)
	if err != nil {
		logger.Sugar().Fatalf("error registering server on etcd : %v", err)
	}

	var g run.Group
	{
		g.Add(func() error {
			for {
				select {
				case <-time.After(1 * time.Second):
					fmt.Println("peer list")
					peers := peerd.GetPeers()
					for _, p := range peers {
						fmt.Println(p)
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}, func(error) {
			peerd.Close()
		})
	}
	{
		g.Add(func() error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			sig := <-c
			cancel()
			return fmt.Errorf("received signal %s", sig)
		}, func(error) {
		})
	}

	err = g.Run()
	logger.Info("exit")
}
