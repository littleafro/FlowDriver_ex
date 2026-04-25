package socks5

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
)

const (
	socksVersion5           = 0x05
	authMethodNoAuth        = 0x00
	authMethodNoAcceptable  = 0xFF
	cmdConnect              = 0x01
	atypIPv4                = 0x01
	atypDomain              = 0x03
	atypIPv6                = 0x04
	replySucceeded          = 0x00
	replyGeneralFailure     = 0x01
	replyCommandUnsupported = 0x07
	replyAddrUnsupported    = 0x08
)

type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

type Server struct {
	Dial DialFunc
}

func (s *Server) ListenAndServe(ctx context.Context, listenAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, client net.Conn) {
	defer client.Close()

	if err := negotiate(client); err != nil {
		return
	}

	targetAddr, cmd, atyp, err := readRequest(client)
	if err != nil {
		return
	}
	if cmd != cmdConnect {
		_ = writeReply(client, replyCommandUnsupported, atyp, nil)
		return
	}
	if s.Dial == nil {
		_ = writeReply(client, replyGeneralFailure, atyp, nil)
		return
	}

	upstream, err := s.Dial(ctx, "tcp", targetAddr)
	if err != nil {
		_ = writeReply(client, replyGeneralFailure, atyp, nil)
		return
	}
	defer upstream.Close()

	_ = writeReply(client, replySucceeded, atyp, upstream.LocalAddr())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, client)
		if tcp, ok := upstream.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, upstream)
		if tcp, ok := client.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	wg.Wait()
}

func negotiate(conn net.Conn) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	if header[0] != socksVersion5 {
		return fmt.Errorf("unsupported SOCKS version %d", header[0])
	}
	nMethods := int(header[1])
	methods := make([]byte, nMethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	for _, method := range methods {
		if method == authMethodNoAuth {
			_, err := conn.Write([]byte{socksVersion5, authMethodNoAuth})
			return err
		}
	}
	_, err := conn.Write([]byte{socksVersion5, authMethodNoAcceptable})
	return err
}

func readRequest(conn net.Conn) (string, byte, byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", 0, 0, err
	}
	if header[0] != socksVersion5 {
		return "", 0, 0, fmt.Errorf("unsupported SOCKS version %d", header[0])
	}
	cmd := header[1]
	atyp := header[3]

	var host string
	switch atyp {
	case atypIPv4:
		addr := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", 0, atyp, err
		}
		host = net.IP(addr).String()
	case atypIPv6:
		addr := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", 0, atyp, err
		}
		host = net.IP(addr).String()
	case atypDomain:
		var ln [1]byte
		if _, err := io.ReadFull(conn, ln[:]); err != nil {
			return "", 0, atyp, err
		}
		addr := make([]byte, int(ln[0]))
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", 0, atyp, err
		}
		host = string(addr)
	default:
		return "", cmd, atyp, fmt.Errorf("unsupported ATYP %d", atyp)
	}

	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", cmd, atyp, err
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	return net.JoinHostPort(host, fmt.Sprintf("%d", port)), cmd, atyp, nil
}

func writeReply(conn net.Conn, rep byte, atyp byte, addr net.Addr) error {
	host := "0.0.0.0"
	port := 0
	if tcpAddr, ok := addr.(*net.TCPAddr); ok && tcpAddr != nil {
		host = tcpAddr.IP.String()
		port = tcpAddr.Port
		if tcpAddr.IP.To4() == nil {
			atyp = atypIPv6
		} else {
			atyp = atypIPv4
		}
	} else {
		atyp = atypIPv4
	}

	reply := []byte{socksVersion5, rep, 0x00, atyp}
	ip := net.ParseIP(host)
	switch atyp {
	case atypIPv4:
		reply = append(reply, ip.To4()...)
	case atypIPv6:
		reply = append(reply, ip.To16()...)
	default:
		reply[3] = atypIPv4
		reply = append(reply, []byte{0, 0, 0, 0}...)
	}
	reply = append(reply, byte(port>>8), byte(port))
	_, err := conn.Write(reply)
	return err
}
