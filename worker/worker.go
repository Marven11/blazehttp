package worker

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	blazehttp "github.com/chaitin/blazehttp/http"

	"github.com/chaitin/blazehttp/testcases"
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
	resultCh        chan *Result
	output          string // output file path
	outputFormat    string // output format: json, csv
	proxy           string // proxy URL
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

func WithOutput(output, format string) WorkerOption {
	return func(w *Worker) {
		w.output = output
		w.outputFormat = format
	}
}

func WithProxy(proxy string) WorkerOption {
	return func(w *Worker) {
		w.proxy = proxy
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

type SampleResult struct {
	FilePath   string `json:"file_path"`
	IsWhite    bool   `json:"is_white"`
	IsPass     bool   `json:"is_pass"`
	StatusCode int    `json:"status_code"`
	TimeCost   int64  `json:"time_cost_ms"`
	Err        string `json:"error,omitempty"`
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
	sampleResults := make([]SampleResult, 0, len(w.fileList))
	var mu sync.Mutex

	go func() {
		w.jobProducer()
	}()

	go func() {
		w.processJobResultWithCollection(&sampleResults, &mu)
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

	if w.output != "" {
		if err := w.exportResults(sampleResults); err != nil {
			fmt.Printf("导出结果失败: %s\n", err)
		} else {
			fmt.Printf("结果已导出到: %s\n", w.output)
		}
	}
}

func (w *Worker) runWorker() {
	for job := range w.jobs {
		func() {
			defer func() {
				w.jobResult <- job
			}()
			filePath := job.FilePath
			req := new(blazehttp.Request)
			if w.useEmbedFS {
				if err := req.ReadFileFromFS(testcases.EmbedTestCasesFS, filePath); err != nil {
					job.Result.Err = fmt.Sprintf("read request file: %s from embed fs error: %s\n", filePath, err)
					return
				}
			} else {
				if err := req.ReadFile(filePath); err != nil {
					job.Result.Err = fmt.Sprintf("read request file: %s error: %s\n", filePath, err)
					return
				}
			}

			if w.reqHost != "" {
				req.SetHost(w.reqHost)
			} else {
				req.SetHost(w.addr)
			}

			if w.reqPerSession {
				// one http request one connection
				req.SetHeader("Connection", "close")
			}

			req.CalculateContentLength()

			start := time.Now()
			conn := blazehttp.Connect(w.addr, w.isHttps, w.timeout, w.proxy)
			if conn == nil {
				job.Result.Err = fmt.Sprintf("connect to %s failed!\n", w.addr)
				return
			}
			nWrite, err := req.WriteTo(*conn)
			if err != nil {
				job.Result.Err = fmt.Sprintf("send request poc: %s length: %d error: %s", filePath, nWrite, err)
				return
			}

			rsp := new(blazehttp.Response)
			if err = rsp.ReadConn(*conn); err != nil {
				job.Result.Err = fmt.Sprintf("read poc file: %s response, error: %s", filePath, err)
				return
			}
			elap := time.Since(start).Nanoseconds()
			(*conn).Close()
			job.Result.Success = true
			if strings.HasSuffix(job.FilePath, "white") {
				job.Result.IsWhite = true // white case
			}

			code := rsp.GetStatusCode()
			job.Result.StatusCode = code
			if code != w.blockStatusCode {
				job.Result.IsPass = true
			}
			job.Result.TimeCost = elap
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

func (w *Worker) processJobResultWithCollection(sampleResults *[]SampleResult, mu *sync.Mutex) {
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

		sample := SampleResult{
			FilePath:   job.FilePath,
			IsWhite:    job.Result.IsWhite,
			IsPass:     job.Result.IsPass,
			StatusCode: job.Result.StatusCode,
			TimeCost:   job.Result.TimeCost / 1000000,
			Err:        job.Result.Err,
		}
		mu.Lock()
		*sampleResults = append(*sampleResults, sample)
		mu.Unlock()
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

func (w *Worker) exportResults(samples []SampleResult) error {
	f, err := os.Create(w.output)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %s", err)
	}
	defer f.Close()

	if w.outputFormat == "json" {
		encoder := json.NewEncoder(f)
		encoder.SetIndent("", "  ")
		return encoder.Encode(samples)
	}

	writer := csv.NewWriter(f)
	defer writer.Flush()

	header := []string{"file_path", "is_white", "is_pass", "status_code", "time_cost_ms", "error"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("写入CSV表头失败: %s", err)
	}

	for _, s := range samples {
		record := []string{
			s.FilePath,
			fmt.Sprintf("%t", s.IsWhite),
			fmt.Sprintf("%t", s.IsPass),
			fmt.Sprintf("%d", s.StatusCode),
			fmt.Sprintf("%d", s.TimeCost),
			s.Err,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("写入CSV记录失败: %s", err)
		}
	}
	return nil
}
