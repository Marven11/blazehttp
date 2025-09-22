package worker

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/chaitin/blazehttp/testcases"

	"golang.org/x/net/proxy"
)

type Progress interface {
	Add(n int) error
}

type Worker struct {
	ctx    context.Context
	cancel context.CancelFunc

	concurrence   int // concurrent connections
	fileList      []string
	jobs          chan *Job
	jobResult     chan *Job
	jobResultDone chan struct{}
	result        *Result
	progressBar   Progress

	addr            string // target addr
	isHttps         bool   // is https
	timeout         int    // connection timeout
	blockStatusCode int    // block status code
	reqHost         string // request host of header
	reqPerSession   bool   // request per session
	useEmbedFS      bool
	proxy           string // proxy address
	resultCh        chan *Result
}

type WorkerOption func(*Worker)

func WithTimeout(timeout int) WorkerOption {
	return func(w *Worker) {
		w.timeout = timeout
	}
}

func WithReqHost(reqHost string) WorkerOption {
	return func(w *Worker) {
		w.reqHost = reqHost
	}
}

func WithReqPerSession(reqPerSession bool) WorkerOption {
	return func(w *Worker) {
		w.reqPerSession = reqPerSession
	}
}

func WithUseEmbedFS(useEmbedFS bool) WorkerOption {
	return func(w *Worker) {
		w.useEmbedFS = useEmbedFS
	}
}

func WithConcurrence(c int) WorkerOption {
	return func(w *Worker) {
		w.concurrence = c
	}
}

func WithResultCh(ch chan *Result) WorkerOption {
	return func(w *Worker) {
		w.resultCh = ch
	}
}

func WithProgressBar(pb Progress) WorkerOption {
	return func(w *Worker) {
		w.progressBar = pb
	}
}

func WithProxy(proxyAddr string) WorkerOption {
	return func(w *Worker) {
		w.proxy = proxyAddr
	}
}

func (w *Worker) Stop() {
	w.cancel()
}

func NewWorker(
	addr string,
	isHttps bool,
	fileList []string,
	blockStatusCode int,
	options ...WorkerOption,
) *Worker {
	w := &Worker{
		concurrence: 10, // default 10

		// payloads
		fileList: fileList,

		// connect target & config
		addr:            addr,
		isHttps:         isHttps,
		timeout:         1000, // 1000ms
		blockStatusCode: blockStatusCode,

		jobs:          make(chan *Job),
		jobResult:     make(chan *Job),
		jobResultDone: make(chan struct{}),

		result: &Result{
			Total: int64(len(fileList)),
		},
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())

	for _, opt := range options {
		opt(w)
	}

	return w
}

type Job struct {
	FilePath string
	Result   *JobResult
}

type JobResult struct {
	IsWhite    bool
	IsPass     bool
	Success    bool
	TimeCost   int64
	StatusCode int
	Err        string
}

type Result struct {
	Total           int64 // total poc
	Error           int64
	Success         int64 // success poc
	SuccessTimeCost int64 // success success cost
	TN              int64
	FN              int64
	TP              int64
	FP              int64
	Job             *Job
}

type Output struct {
	Out string
	Err string
}

func (w *Worker) Run() {
	go func() {
		w.jobProducer()
	}()

	go func() {
		w.processJobResult()
		w.jobResultDone <- struct{}{}
	}()

	wg := sync.WaitGroup{}

	for i := 0; i < w.concurrence; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runWorker()
		}()
	}
	wg.Wait()

	close(w.jobResult)
	<-w.jobResultDone

	fmt.Println(w.generateResult())
}

func fixHostHeader(rawRequest []byte, hostHeader string) []byte {
	re := regexp.MustCompile(`(?i)(Host:.*\r\n)`)
	return re.ReplaceAll(rawRequest, []byte(fmt.Sprintf("Host: %s\r\n", hostHeader)))
}

func (w *Worker) readRequestFromFile(filePath string) ([]byte, error) {
	var data []byte
	var err error
	if w.useEmbedFS {
		data, err = testcases.EmbedTestCasesFS.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read request file: %s from embed fs error: %s", filePath, err)
		}
	} else {
		data, err = os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("read request file: %s error: %v", filePath, err)
		}
	}

	return fixHostHeader(data, w.addr), nil
}
func rewriteRequestRfc7230(host string, rawRequest []byte) []byte {
	scheme := []byte("http")

	lines := bytes.SplitN(rawRequest, []byte("\n"), 2)
	if len(lines) < 2 {
		return rawRequest
	}

	parts := bytes.Fields(lines[0])
	if len(parts) < 3 {
		return rawRequest
	}

	newFirstLine := append([]byte(parts[0]), []byte(" ")...)
	newFirstLine = append(newFirstLine, scheme...)
	newFirstLine = append(newFirstLine, []byte("://")...)
	newFirstLine = append(newFirstLine, []byte(host)...)
	newFirstLine = append(newFirstLine, parts[1]...)
	newFirstLine = append(newFirstLine, []byte(" ")...)
	newFirstLine = append(newFirstLine, parts[2]...)
	newFirstLine = append(newFirstLine, []byte("\r\n")...)

	result := append(newFirstLine, lines[1]...)
	return result
}

func (w *Worker) doConnect(rawRequest []byte) (conn net.Conn, request []byte, err error) {

	request = rawRequest

	// 解析目标地址
	addr := w.addr
	host, port, err := net.SplitHostPort(w.addr)
	if err != nil {
		return nil, nil, err
	}

	if port == "" {
		if w.isHttps {
			addr = host + ":443"
		} else {
			addr = host + ":80"
		}
	}

	if w.proxy == "" {
		conn, err = net.Dial("tcp", addr)
		if err != nil {
			return nil, nil, fmt.Errorf("dial error: %v", err)
		}
	} else if w.proxy != "" {
		proxyUrl, err := url.Parse(w.proxy)

		if err != nil {
			return nil, nil, err
		}

		if proxyUrl.Scheme == "http" {
			conn, err = net.Dial("tcp", proxyUrl.Host)
			if err != nil {
				return nil, nil, fmt.Errorf("connect proxy error: %v", err)
			}
			if w.isHttps {
				fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\\r\\nHost: %s\\r\\n\\r\\n", addr, addr)
				reader := bufio.NewReader(conn)
				line, err := reader.ReadString('\n')
				if err != nil || !strings.HasPrefix(line, "HTTP/1.1 200") {
					conn.Close()
					return nil, nil, fmt.Errorf("CONNECT failed: %s", line)
				}
				for {
					line, err := reader.ReadString('\n')
					if err != nil || line == "\\r\\n" {
						break
					}
				}
			} else {
				request = rewriteRequestRfc7230(w.addr, rawRequest)
			}
		} else if proxyUrl.Scheme == "socks5" {
			dialer, err := proxy.SOCKS5("tcp", proxyUrl.Host, nil, proxy.Direct)
			if err != nil {
				return nil, nil, fmt.Errorf("SOCKS dialer error: %v", err)
			}
			conn, err = dialer.Dial("tcp", w.addr)
			if err != nil {
				return nil, nil, fmt.Errorf("SOCKS connect error: %v", err)
			}
		} else {
			return nil, nil, fmt.Errorf("unsupported proxy type %s", proxyUrl.Scheme)
		}
	}

	// 处理TLS
	if w.isHttps {
		config := &tls.Config{ServerName: host}
		tlsConn := tls.Client(conn, config)
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("TLS handshake error: %v", err)
		}
		conn = tlsConn
	}

    timeout := time.Duration(w.timeout) * time.Millisecond
    conn.SetReadDeadline(time.Now().Add(timeout))
    conn.SetWriteDeadline(time.Now().Add(timeout))

	return conn, request, nil
}

func (w *Worker) runWorker() {
	for job := range w.jobs {
		func() {
			defer func() {
				w.jobResult <- job
			}()
			filePath := job.FilePath
			rawRequest, err := w.readRequestFromFile(filePath)
			if err != nil {
				job.Result.Err = fmt.Sprintf("%s\n", err)
				return
			}
			conn, request, err := w.doConnect(rawRequest)
			if err != nil {
				job.Result.Err = fmt.Sprintf("%s\n", err)
				return
			}
			defer conn.Close()
			start := time.Now()
			_, err = conn.Write([]byte(request))
			if err != nil {
				conn.Close()
				job.Result.Err = fmt.Sprintf("write request error: %v", err)
				return
			}

			reader := bufio.NewReader(conn)
			resp, err := http.ReadResponse(reader, nil)
			if err != nil {
				conn.Close()
				job.Result.Err = fmt.Sprintf("read response error: %v", err)
				return
			}
			defer resp.Body.Close()
			elap := time.Since(start).Nanoseconds()

			job.Result.Success = true
			if strings.HasSuffix(job.FilePath, "white") {
				job.Result.IsWhite = true
			}

			job.Result.StatusCode = resp.StatusCode
			if resp.StatusCode != w.blockStatusCode {
				job.Result.IsPass = true
			}
			job.Result.TimeCost = elap
			conn.Close()
		}()
	}
}

func (w *Worker) processJobResult() {
	for job := range w.jobResult {
		if job.Result.Success {
			w.result.Success++
			w.result.SuccessTimeCost += job.Result.TimeCost
			if job.Result.IsWhite {
				if job.Result.IsPass {
					w.result.TN++
				} else {
					w.result.FP++
				}
			} else {
				if job.Result.IsPass {
					w.result.FN++
				} else {
					w.result.TP++
				}
			}
		} else {
			w.result.Error++
		}
		if w.resultCh != nil {
			r := *w.result
			r.Job = job
			w.resultCh <- &r
		}
	}
}

func (w *Worker) jobProducer() {
	defer close(w.jobs)
	for _, f := range w.fileList {
		select {
		case <-w.ctx.Done():
			return
		default:
			w.jobs <- &Job{
				FilePath: f,
				Result:   &JobResult{},
			}
			if w.progressBar != nil {
				_ = w.progressBar.Add(1)
			}
		}
	}
}

func (w *Worker) generateResult() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("总样本数量: %d    成功: %d    错误: %d\n", w.result.Total, w.result.Success, (w.result.Total - w.result.Success)))
	sb.WriteString(fmt.Sprintf("检出率: %.2f%% (恶意样本总数: %d , 正确拦截: %d , 漏报放行: %d)\n", float64(w.result.TP)*100/float64(w.result.TP+w.result.FN), w.result.TP+w.result.FN, w.result.TP, w.result.FN))
	sb.WriteString(fmt.Sprintf("误报率: %.2f%% (正常样本总数: %d , 正确放行: %d , 误报拦截: %d)\n", float64(w.result.FP)*100/float64(w.result.TN+w.result.FP), w.result.TN+w.result.FP, w.result.TN, w.result.FP))
	sb.WriteString(fmt.Sprintf("准确率: %.2f%% (正确拦截 + 正确放行）/样本总数 \n", float64(w.result.TP+w.result.TN)*100/float64(w.result.TP+w.result.TN+w.result.FP+w.result.FN)))
	sb.WriteString(fmt.Sprintf("平均耗时: %.2f毫秒\n", float64(w.result.SuccessTimeCost)/float64(w.result.Success)/1000000))
	return sb.String()
}
