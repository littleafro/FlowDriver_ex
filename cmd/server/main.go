package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/netutil"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Default path to Google credentials JSON")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	pool, err := app.BuildBackendPool(ctx, appCfg, configPath, gcPath)
	if err != nil {
		log.Fatalf("failed to initialize backend pool: %v", err)
	}

	engine := transport.NewEngineWithPool(pool, false, "", app.BuildEngineOptions(appCfg))

	allowCIDRs, err := netutil.ParseCIDRs(appCfg.AllowCIDRs)
	if err != nil {
		log.Fatalf("invalid allow_cidrs: %v", err)
	}
	denyCIDRs, err := netutil.ParseCIDRs(appCfg.DenyCIDRs)
	if err != nil {
		log.Fatalf("invalid deny_cidrs: %v", err)
	}
	policy := &netutil.DialPolicy{
		AllowCIDRs:      allowCIDRs,
		DenyCIDRs:       denyCIDRs,
		BlockPrivateIPs: appCfg.PrivateIPsBlocked(),
		DialTimeout:     time.Duration(appCfg.DialTimeoutMs) * time.Millisecond,
		KeepAlive:       time.Duration(appCfg.TCPKeepAliveMs) * time.Millisecond,
	}

	engine.OnNewSession = func(sessionID, targetAddr string, session *transport.Session) {
		log.Printf("server session open id=%s backend=%s target=%s", sessionID, session.BackendName, targetAddr)
		go handleServerConn(ctx, session, policy)
	}
	engine.Start(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
}

func handleServerConn(ctx context.Context, session *transport.Session, policy *netutil.DialPolicy) {
	conn, err := policy.DialContext(ctx, session.TargetAddr)
	if err != nil {
		log.Printf("dial error target=%s session=%s: %v", session.TargetAddr, session.ID, err)
		session.QueueClose()
		return
	}
	defer conn.Close()

	errCh := make(chan error, 2)

	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				session.EnqueueTx(buf[:n])
			}
			if err != nil {
				if err == io.EOF {
					session.QueueClose()
				}
				errCh <- err
				return
			}
		}
	}()

	go func() {
		for {
			data, ok := <-session.RxChan
			if !ok {
				errCh <- fmt.Errorf("session closed by remote")
				return
			}
			if len(data) == 0 {
				continue
			}
			if _, err := conn.Write(data); err != nil {
				errCh <- err
				return
			}
		}
	}()

	<-errCh
	session.QueueClose()
}
