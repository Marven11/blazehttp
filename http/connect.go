package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/net/proxy"
)

func Connect(addr string, isHttps bool, timeout int, proxyURL string) *net.Conn {
	var n net.Conn
	var err error
	if m, _ := regexp.MatchString(`.*(]:)|(:)[0-9]+$`, addr); !m {
		if isHttps {
			addr = fmt.Sprintf("%s:443", addr)
		} else {
			addr = fmt.Sprintf("%s:80", addr)
		}
	}
	retryCnt := 0
retry:
	if proxyURL != "" {
		n, err = dialWithProxy(addr, proxyURL, timeout)
	} else if isHttps {
		n, err = tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		n, err = net.Dial("tcp", addr)
	}
	if err != nil {
		retryCnt++
		if retryCnt < 4 {
			goto retry
		} else {
			return nil
		}
	}
	wDeadline := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	rDeadline := time.Now().Add(time.Duration(timeout*2) * time.Millisecond)
	deadline := time.Now().Add(time.Duration(timeout*2) * time.Millisecond)
	_ = n.SetDeadline(deadline)
	_ = n.SetReadDeadline(rDeadline)
	_ = n.SetWriteDeadline(wDeadline)

	return &n
}

func dialWithProxy(addr, proxyURL string, timeout int) (net.Conn, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("解析代理URL失败: %s", err)
	}

	switch u.Scheme {
	case "socks5":
		dialer, err := proxy.SOCKS5("tcp", u.Host, nil, proxy.Direct)
		if err != nil {
			return nil, fmt.Errorf("创建SOCKS5代理失败: %s", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()
		return dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", addr)
	case "socks5h":
		return dialSocks5WithRemoteDNS(u.Host, addr, timeout)
	case "http", "https":
		conn, err := net.DialTimeout("tcp", u.Host, time.Duration(timeout)*time.Millisecond)
		if err != nil {
			return nil, fmt.Errorf("连接HTTP代理失败: %s", err)
		}
		connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", addr, addr)
		if _, err := conn.Write([]byte(connectReq)); err != nil {
			conn.Close()
			return nil, fmt.Errorf("发送CONNECT请求失败: %s", err)
		}
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("读取CONNECT响应失败: %s", err)
		}
		response := string(buf[:n])
		if len(response) < 12 || response[9] != '2' {
			conn.Close()
			return nil, fmt.Errorf("CONNECT请求失败: %s", response)
		}
		return conn, nil
	default:
		return nil, fmt.Errorf("不支持的代理协议: %s", u.Scheme)
	}
}

func dialSocks5WithRemoteDNS(proxyHost, targetAddr string, timeout int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", proxyHost, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("连接SOCKS5代理失败: %s", err)
	}
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		conn.Close()
		return nil, err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}
	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, []byte(host)...)
	req = append(req, byte(port>>8), byte(port&0xff))
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}
	resp2 := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp2); err != nil {
		conn.Close()
		return nil, err
	}
	if resp2[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("SOCKS5连接失败, 错误码: %d", resp2[1])
	}
	switch resp2[3] {
	case 0x01:
		buf := make([]byte, 4)
		io.ReadFull(conn, buf)
	case 0x03:
		lenBuf := make([]byte, 1)
		io.ReadFull(conn, lenBuf)
		buf := make([]byte, lenBuf[0])
		io.ReadFull(conn, buf)
	case 0x04:
		buf := make([]byte, 16)
		io.ReadFull(conn, buf)
	}
	portBuf := make([]byte, 2)
	io.ReadFull(conn, portBuf)
	return conn, nil
}
