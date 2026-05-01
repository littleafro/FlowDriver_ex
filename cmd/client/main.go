package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
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
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

func generateSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type rawResolver struct{}

func (rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

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

	cid := appCfg.ClientID
	if cid == "" {
		cid = generateSessionID()[:8]
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
	engine := transport.NewEngineWithPool(pool, true, cid, engineOpts)
	if adaptive {
		controller.Attach(engine, pool, nil)
		controller.Start(ctx)
	}
	engine.Start(ctx)

	rawCIDRs, err := netutil.ParseCIDRs(appCfg.AllowedRawIPCidrs)
	if err != nil {
		log.Fatalf("invalid allowed_raw_ip_cidrs: %v", err)
	}
	rawPolicy := &netutil.DialPolicy{AllowedRawCIDRs: rawCIDRs}

	server := socks5.NewServer(
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sessionID := generateSessionID()
			session := transport.NewSession(sessionID)
			session.ClientID = cid
			session.TargetAddr = addr

			host, port, err := net.SplitHostPort(addr)
			if err == nil {
				isRaw, allowed, evalErr := app.EvaluateRawIPPolicy(appCfg, rawPolicy, addr)
				if isRaw {
					if appCfg.RawIPWarned() {
						log.Printf("raw IP warning session=%s target=%s:%s allowed=%t", sessionID, host, port, allowed)
					}
					if evalErr != nil {
						return nil, evalErr
					}
				} else {
					log.Printf("new session %s target=%s:%s", sessionID, host, port)
				}
			} else {
				log.Printf("new session %s target=%s", sessionID, addr)
			}

			engine.AddSession(session)
			return transport.NewVirtualConn(session, engine), nil
		}),
		socks5.WithAssociateHandle(func(ctx context.Context, w io.Writer, req *socks5.Request) error {
			socks5.SendReply(w, statute.RepCommandNotSupported, nil)
			return fmt.Errorf("covert UDP not supported")
		}),
		socks5.WithResolver(rawResolver{}),
	)

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = config.DefaultListenAddr
	}
	log.Printf("listening for SOCKS5 on %s", listenAddr)

	go func() {
		if err := server.ListenAndServe("tcp", listenAddr); err != nil {
			log.Fatalf("SOCKS5 server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	engine.Shutdown(shutdownCtx)
}
