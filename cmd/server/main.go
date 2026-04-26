package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
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
	shutdownCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	engine.Shutdown(shutdownCtx)
}

func handleServerConn(ctx context.Context, session *transport.Session, policy *netutil.DialPolicy) {
	conn, err := policy.DialContext(ctx, session.TargetAddr)
	if err != nil {
		log.Printf("dial error target=%s session=%s: %v", session.TargetAddr, session.ID, err)
		session.QueueClose()
		return
	}
	defer conn.Close()

	virtual := transport.NewVirtualConn(session, nil)
	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(virtual, conn)
		if err == nil {
			err = io.EOF
		}
		_ = virtual.CloseWrite()
		errCh <- err
	}()

	go func() {
		_, err := io.Copy(conn, virtual)
		if err == nil {
			err = io.EOF
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		errCh <- err
	}()

	<-errCh
	session.QueueClose()
}
