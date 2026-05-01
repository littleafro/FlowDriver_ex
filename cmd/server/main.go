package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/netutil"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func main() {
	var configPath, gcPath string
	var adaptive bool
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Default path to Google credentials JSON")
	flag.BoolVar(&adaptive, "adaptive", false, "Enable adaptive mode to optimize performance parameters")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	var (
		pool       *storage.BackendPool
		controller *app.AdaptiveController
	)
	if adaptive {
		controller, err = app.NewAdaptiveController(appCfg.DataDir)
		if err != nil {
			log.Fatalf("failed to initialize adaptive controller: %v", err)
		}
		pool, err = app.BuildAdaptiveBackendPool(ctx, appCfg, configPath, gcPath, controller)
	} else {
		pool, err = app.BuildBackendPool(ctx, appCfg, configPath, gcPath)
	}
	if err != nil {
		log.Fatalf("failed to initialize backend pool: %v", err)
	}

	engineOpts := app.BuildEngineOptions(appCfg)
	if adaptive {
		engineOpts = controller.InitialEngineOptions(appCfg)
	}
	engine := transport.NewEngineWithPool(pool, false, appCfg.ClientID, engineOpts)

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
	if adaptive {
		controller.Attach(engine, pool, policy)
		controller.Start(ctx)
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

	virtual := transport.NewVirtualConn(session, nil)
	var wg sync.WaitGroup
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = conn.Close()
			_ = virtual.Close()
		})
	}
	wg.Add(2)

	go func() {
		<-ctx.Done()
		closeBoth()
	}()

	go func() {
		defer wg.Done()
		_, err := io.Copy(virtual, conn)
		if err != nil && err != io.EOF {
			log.Printf("copy upstream->session error target=%s session=%s: %v", session.TargetAddr, session.ID, err)
			closeBoth()
		}
		_ = virtual.CloseWrite()
	}()

	go func() {
		defer wg.Done()
		_, err := io.Copy(conn, virtual)
		if err != nil && err != io.EOF {
			log.Printf("copy session->upstream error target=%s session=%s: %v", session.TargetAddr, session.ID, err)
			closeBoth()
		}
		if closer, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
	}()

	wg.Wait()
	closeBoth()
	session.QueueClose()
}
