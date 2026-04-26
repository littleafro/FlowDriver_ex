package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/netutil"
	covertsocks "github.com/NullLatency/flow-driver/internal/socks5"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func generateSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

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

	cid := appCfg.ClientID
	if cid == "" {
		cid = generateSessionID()[:8]
	}

	engine := transport.NewEngineWithPool(pool, true, cid, app.BuildEngineOptions(appCfg))
	engine.Start(ctx)

	rawCIDRs, err := netutil.ParseCIDRs(appCfg.AllowedRawIPCidrs)
	if err != nil {
		log.Fatalf("invalid allowed_raw_ip_cidrs: %v", err)
	}
	rawPolicy := &netutil.DialPolicy{AllowedRawCIDRs: rawCIDRs}

	server := &covertsocks.Server{
		Dial: func(dc context.Context, network, addr string) (net.Conn, error) {
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
		},
	}

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = config.DefaultListenAddr
	}
	log.Printf("listening for SOCKS5 on %s", listenAddr)

	go func() {
		if err := server.ListenAndServe(ctx, listenAddr); err != nil {
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
