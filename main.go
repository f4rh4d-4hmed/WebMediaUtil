package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	"golang.org/x/sync/semaphore"
)

// TaskRequest is the request structure
type TaskRequest struct {
	URL          string            `json:"url"`
	Mode         string            `json:"mode,omitempty"`
	WaitMs       int               `json:"wait_ms"`
	LocalStorage map[string]string `json:"local_storage,omitempty"`
	Actions      []BrowserAction   `json:"actions,omitempty"`
	Debug        bool              `json:"debug,omitempty"`
	IncludeHeaders bool            `json:"include_headers,omitempty"`
	Stream       bool              `json:"stream,omitempty"`
}

type BrowserAction struct {
	Type     string  `json:"type"`
	Selector string  `json:"selector,omitempty"`
	Script   string  `json:"script,omitempty"`
	Text     string  `json:"text,omitempty"`
	X        float64 `json:"x,omitempty"`
	Y        float64 `json:"y,omitempty"`
	DeltaX   float64 `json:"delta_x,omitempty"`
	DeltaY   float64 `json:"delta_y,omitempty"`
	WaitMs   int     `json:"wait_ms,omitempty"`
}

// TaskResponse is what we send back
type TaskResponse struct {
	Content     string              `json:"content"`
	M3u8URLs    []string            `json:"m3u8_urls,omitempty"`
	AllURLs     []string            `json:"all_urls,omitempty"`
	M3u8Headers []CapturedURLHeader `json:"m3u8_headers,omitempty"`
	Error       string              `json:"error,omitempty"`
}

type CapturedURLHeader struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Status  int               `json:"status,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Error   string            `json:"error,omitempty"`
}

type StreamEvent struct {
	Type     string        `json:"type"`
	URL      string        `json:"url,omitempty"`
	IsMedia  bool          `json:"is_media,omitempty"`
	Response *TaskResponse `json:"response,omitempty"`
	Error    string        `json:"error,omitempty"`
}

const (
	maxConcurrentTabs = 5
	requestTimeout    = 120 * time.Second
	serverAddr        = ":8080"
	maxHeaderProbes   = 8
	headerProbeTimeout = 12 * time.Second
)

var nextExtensionJobID atomic.Uint64

type extensionCaptureEvent struct {
	JobID     string `json:"job_id,omitempty"`
	URL       string `json:"url"`
	TabID     int    `json:"tab_id"`
	FrameID   int    `json:"frame_id"`
	RequestID string `json:"request_id"`
	Type      string `json:"type"`
	Initiator string `json:"initiator,omitempty"`
}

type extensionCaptureSession struct {
	jobID    string
	startURL string
	hasTabID bool
	tabID    int
	trackURL func(string)
}

type extensionCaptureHub struct {
	mu       sync.Mutex
	sessions map[*extensionCaptureSession]struct{}
}

type extensionJob struct {
	JobID        string            `json:"job_id"`
	URL          string            `json:"url"`
	WaitMs       int               `json:"wait_ms"`
	LocalStorage map[string]string `json:"local_storage,omitempty"`
	Actions      []BrowserAction   `json:"actions,omitempty"`
	CloseTab     bool              `json:"close_tab"`
}

type extensionJobResult struct {
	JobID   string `json:"job_id"`
	Content string `json:"content"`
	Error   string `json:"error,omitempty"`
}

type extensionJobHub struct {
	mu      sync.Mutex
	queue   []*extensionJob
	results map[string]chan extensionJobResult
}

type extensionBrowserProcess struct {
	cmd        *exec.Cmd
	profileDir string
}

func newExtensionCaptureHub() *extensionCaptureHub {
	return &extensionCaptureHub{
		sessions: make(map[*extensionCaptureSession]struct{}),
	}
}

func newExtensionJobHub() *extensionJobHub {
	return &extensionJobHub{
		results: make(map[string]chan extensionJobResult),
	}
}

func (h *extensionJobHub) enqueue(job *extensionJob) <-chan extensionJobResult {
	resultCh := make(chan extensionJobResult, 1)

	h.mu.Lock()
	h.queue = append(h.queue, job)
	h.results[job.JobID] = resultCh
	h.mu.Unlock()

	return resultCh
}

func (h *extensionJobHub) next() (*extensionJob, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.queue) == 0 {
		return nil, false
	}

	job := h.queue[0]
	copy(h.queue, h.queue[1:])
	h.queue[len(h.queue)-1] = nil
	h.queue = h.queue[:len(h.queue)-1]
	return job, true
}

func (h *extensionJobHub) complete(result extensionJobResult) bool {
	h.mu.Lock()
	resultCh := h.results[result.JobID]
	delete(h.results, result.JobID)
	h.mu.Unlock()

	if resultCh == nil {
		return false
	}
	resultCh <- result
	return true
}

func (h *extensionJobHub) cancel(jobID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.results, jobID)
	for i, job := range h.queue {
		if job.JobID == jobID {
			copy(h.queue[i:], h.queue[i+1:])
			h.queue[len(h.queue)-1] = nil
			h.queue = h.queue[:len(h.queue)-1]
			return
		}
	}
}

func (h *extensionCaptureHub) register(jobID, startURL string, trackURL func(string)) func() {
	session := &extensionCaptureSession{
		jobID:    jobID,
		startURL: normalizeCapturedURL(startURL, ""),
		trackURL: trackURL,
	}

	h.mu.Lock()
	h.sessions[session] = struct{}{}
	h.mu.Unlock()

	return func() {
		h.mu.Lock()
		delete(h.sessions, session)
		h.mu.Unlock()
	}
}

func (h *extensionCaptureHub) capture(ev extensionCaptureEvent) {
	if ev.URL == "" || ev.TabID < 0 {
		return
	}

	normalizedURL := normalizeCapturedURL(ev.URL, "")
	var trackers []func(string)

	h.mu.Lock()
	for session := range h.sessions {
		if session.jobID != "" {
			if session.jobID == ev.JobID {
				trackers = append(trackers, session.trackURL)
			}
			continue
		}

		if session.hasTabID {
			if session.tabID == ev.TabID {
				trackers = append(trackers, session.trackURL)
			}
			continue
		}

		if ev.Type == "main_frame" && sameCapturedURL(normalizedURL, session.startURL) {
			session.hasTabID = true
			session.tabID = ev.TabID
			trackers = append(trackers, session.trackURL)
		}
	}
	h.mu.Unlock()

	for _, trackURL := range trackers {
		trackURL(ev.URL)
	}
}

func ensureCaptureExtension() (string, error) {
	dir := filepath.Join(os.TempDir(), "shadoware-capture-extension")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	manifest := `{
  "manifest_version": 3,
  "name": "ShadoWare Capture",
  "version": "1.0.0",
  "description": "Extension-side browser control and passive request URL capture for ShadoWare.",
  "permissions": ["webRequest", "tabs", "scripting"],
  "host_permissions": ["<all_urls>"],
  "background": {
    "service_worker": "background.js"
  }
}
`
	apiBase := "http://127.0.0.1" + serverAddr
	background := fmt.Sprintf(`const API_BASE = %q;
const CAPTURE_ENDPOINT = API_BASE + "/extension-capture";
const COMMAND_ENDPOINT = API_BASE + "/extension-command";
const RESULT_ENDPOINT = API_BASE + "/extension-result";
const tabJobs = new Map();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function postJSON(url, body) {
  await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
}

chrome.webRequest.onBeforeRequest.addListener((details) => {
  if (!details.url || details.url.startsWith(CAPTURE_ENDPOINT)) return;
  const jobId = tabJobs.get(details.tabId);
  if (!jobId) return;

  postJSON(CAPTURE_ENDPOINT, {
    job_id: jobId,
    url: details.url,
    tab_id: details.tabId,
    frame_id: details.frameId,
    request_id: details.requestId,
    type: details.type,
    initiator: details.initiator || ""
  }).catch(() => {});
}, { urls: ["<all_urls>"] });

function waitForTabComplete(tabId, timeoutMs = 30000) {
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      chrome.tabs.onUpdated.removeListener(listener);
      resolve();
    };
    const listener = (updatedTabId, info) => {
      if (updatedTabId === tabId && info.status === "complete") finish();
    };
    chrome.tabs.onUpdated.addListener(listener);
    chrome.tabs.get(tabId).then((tab) => {
      if (tab.status === "complete") finish();
    }).catch(finish);
    setTimeout(finish, timeoutMs);
  });
}

async function runAction(tabId, action) {
  const type = String(action.type || "").toLowerCase();
  if (type === "wait" || type === "sleep") {
    await sleep(action.wait_ms || 0);
    return;
  }

  if (type === "wait_ready") {
    const selector = action.selector;
    const timeout = action.wait_ms || 10000;
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [selector, timeout],
      func: async (selector, timeout) => {
        const start = Date.now();
        while (!document.querySelector(selector) && Date.now() - start < timeout) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
    });
    return;
  }

  if (type === "click" || type === "double_click") {
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [action.selector || "", action.x || 0, action.y || 0, type === "double_click"],
      func: (selector, x, y, doubleClick) => {
        const el = selector ? document.querySelector(selector) : document.elementFromPoint(x, y);
        if (!el) return;
        el.dispatchEvent(new MouseEvent("mousedown", { bubbles: true, cancelable: true, view: window }));
        el.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, cancelable: true, view: window }));
        el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window }));
        if (doubleClick) {
          el.dispatchEvent(new MouseEvent("dblclick", { bubbles: true, cancelable: true, view: window }));
        }
      }
    });
    return;
  }

  if (type === "scroll") {
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [action.delta_x || 0, action.delta_y || 0],
      func: (deltaX, deltaY) => window.scrollBy(deltaX, deltaY)
    });
    return;
  }

  if (type === "send_keys" || type === "type") {
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [action.selector, action.text || ""],
      func: (selector, text) => {
        const el = document.querySelector(selector);
        if (!el) return;
        el.focus();
        el.value = text;
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
      }
    });
    return;
  }

  if (type === "evaluate" || type === "eval") {
    await chrome.scripting.executeScript({
      target: { tabId },
      args: [action.script || ""],
      world: "MAIN",
      func: (script) => {
        (0, eval)(script);
      }
    });
  }
}

async function runJob(job) {
  let tab;
  try {
    tab = await chrome.tabs.create({ url: job.url, active: true });
    tabJobs.set(tab.id, job.job_id);
    await waitForTabComplete(tab.id);

    if (job.local_storage && Object.keys(job.local_storage).length) {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        args: [job.local_storage],
        func: (items) => {
          for (const [key, value] of Object.entries(items)) {
            localStorage.setItem(key, value);
          }
        }
      });
      await chrome.tabs.reload(tab.id);
      await waitForTabComplete(tab.id);
    }

    for (const action of job.actions || []) {
      await runAction(tab.id, action);
    }

    if (job.wait_ms) await sleep(job.wait_ms);

    const frames = await chrome.scripting.executeScript({
      target: { tabId: tab.id, allFrames: true },
      func: () => document.documentElement.outerHTML
    });
    const content = frames.map((frame) => frame.result || "").join("\n");
    await postJSON(RESULT_ENDPOINT, {
      job_id: job.job_id,
      content
    });
  } catch (e) {
    await postJSON(RESULT_ENDPOINT, {
      job_id: job.job_id,
      content: "",
      error: e && e.message ? e.message : String(e)
    }).catch(() => {});
  } finally {
    if (tab && tab.id !== undefined) {
      tabJobs.delete(tab.id);
      if (job.close_tab !== false) {
        chrome.tabs.remove(tab.id).catch(() => {});
      }
    }
  }
}

async function pollCommands() {
  for (;;) {
    try {
      const response = await fetch(COMMAND_ENDPOINT);
      if (response.status === 200) {
        await runJob(await response.json());
      } else {
        await sleep(500);
      }
    } catch (_) {
      await sleep(1000);
    }
  }
}

pollCommands();
`, apiBase)

	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "background.js"), []byte(background), 0644); err != nil {
		return "", err
	}
	return dir, nil
}

func launchExtensionBrowser(browserPath, extensionDir, jobID string) (*extensionBrowserProcess, error) {
	profileDir, err := os.MkdirTemp("", "shadoware-browser-profile-"+safeFilePart(jobID)+"-")
	if err != nil {
		return nil, err
	}

	args := []string{
		"--user-data-dir=" + profileDir,
		"--headless=new",
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-gpu",
		"--disable-blink-features=AutomationControlled",
		"--disable-extensions-except=" + extensionDir,
		"--load-extension=" + extensionDir,
		"--window-size=1365,768",
		"--mute-audio",
		"about:blank",
	}

	cmd := exec.Command(browserPath, args...)
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(profileDir)
		return nil, err
	}
	return &extensionBrowserProcess{cmd: cmd, profileDir: profileDir}, nil
}

func closeExtensionBrowser(browser *extensionBrowserProcess) {
	if browser == nil {
		return
	}
	if browser.cmd != nil && browser.cmd.Process != nil {
		if runtime.GOOS == "windows" {
			_ = exec.Command("taskkill", "/PID", strconv.Itoa(browser.cmd.Process.Pid), "/T", "/F").Run()
		} else {
			_ = browser.cmd.Process.Kill()
		}

		done := make(chan struct{})
		go func() {
			_ = browser.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	if browser.profileDir != "" && strings.HasPrefix(filepath.Base(browser.profileDir), "shadoware-browser-profile-") {
		_ = os.RemoveAll(browser.profileDir)
	}
}

func safeFilePart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "job"
	}
	return b.String()
}

func main() {
	browserPath, browserName := findBrowser()
	if browserPath == "" {
		log.Fatal("No Chromium-based browser found. Install Chrome, Edge, or Brave.")
	}
	log.Printf("Using browser: %s (%s)", browserName, browserPath)

	captureExtensionDir, err := ensureCaptureExtension()
	if err != nil {
		log.Fatalf("Failed to prepare capture extension: %v", err)
	}
	log.Printf("Capture extension loaded from %s", captureExtensionDir)

	captureHub := newExtensionCaptureHub()
	jobHub := newExtensionJobHub()

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(browserPath),
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		// chromedp.Headless, // Commented out for visual debugging; re-enable for production
		chromedp.Flag("headless", false), // Explicitly override the default headless=true
		chromedp.DisableGPU,
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-extensions", false),
		chromedp.Flag("disable-extensions-except", captureExtensionDir),
		chromedp.Flag("load-extension", captureExtensionDir),
		chromedp.Flag("disable-plugins", true),
		// NOTE: "disable-images" and "disable-background-networking" are intentionally
		// omitted; they suppress the network events we need for m3u8 capture.
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("safebrowsing-disable-auto-update", true),
		chromedp.Flag("js-flags", "--max-old-space-size=128"),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()

	sem := semaphore.NewWeighted(maxConcurrentTabs)

	mux := http.NewServeMux()
	mux.HandleFunc("/execute", handleExecute(allocCtx, sem, captureHub, jobHub, browserPath, captureExtensionDir))
	mux.HandleFunc("/extension-command", handleExtensionCommand(jobHub))
	mux.HandleFunc("/extension-capture", handleExtensionCapture(captureHub))
	mux.HandleFunc("/extension-result", handleExtensionResult(jobHub))
	mux.HandleFunc("/health", handleHealth)

	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      withCORS(withLogging(mux)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: requestTimeout + 5*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := func(reason string) {
		log.Printf("Shutting down: %s", reason)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Forced shutdown: %v", err)
		}
	}

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		sig := <-quit
		shutdown(fmt.Sprintf("signal %s received", sig))
	}()

	startParentWatchdog(shutdown)

	fmt.Printf("Shadoware active on http://localhost%s (parent PID: %d)\n", serverAddr, os.Getppid())
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}

func handleExecute(allocCtx context.Context, sem *semaphore.Weighted, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var req TaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := validateRequest(req); err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()

		if !sem.TryAcquire(1) {
			writeError(w, "Server busy, try again shortly", http.StatusServiceUnavailable)
			return
		}
		defer sem.Release(1)

		if req.Stream {
			handleExecuteStream(w, r, ctx, allocCtx, req, captureHub, jobHub, browserPath, extensionDir)
			return
		}

		var (
			content string
			m3u8s   []string
			allURLs []string
			err     error
		)
		if strings.EqualFold(req.Mode, "cdp") {
			content, m3u8s, allURLs, err = scrapeCDP(ctx, allocCtx, req, captureHub, nil)
		} else {
			content, m3u8s, allURLs, err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, nil)
		}

		resp := TaskResponse{Content: content, M3u8URLs: m3u8s}
		if req.Debug {
			resp.AllURLs = allURLs
		}
		if req.Debug || req.IncludeHeaders {
			resp.M3u8Headers = collectM3U8Headers(ctx, m3u8s)
		}
		if err != nil {
			resp.Error = err.Error()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

type scrapeResult struct {
	content string
	m3u8s   []string
	allURLs []string
	err     error
}

func handleExecuteStream(w http.ResponseWriter, r *http.Request, ctx context.Context, allocCtx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, "streaming is not supported by this server", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	events := make(chan StreamEvent, 256)
	emit := func(ev StreamEvent) {
		select {
		case events <- ev:
		default:
		}
	}
	results := make(chan scrapeResult, 1)

	go func() {
		var res scrapeResult
		if strings.EqualFold(req.Mode, "cdp") {
			res.content, res.m3u8s, res.allURLs, res.err = scrapeCDP(ctx, allocCtx, req, captureHub, emit)
		} else {
			res.content, res.m3u8s, res.allURLs, res.err = scrapeExtension(ctx, req, captureHub, jobHub, browserPath, extensionDir, emit)
		}
		results <- res
	}()

	writeEvent := func(ev StreamEvent) bool {
		if err := json.NewEncoder(w).Encode(ev); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	for {
		select {
		case ev := <-events:
			if !writeEvent(ev) {
				return
			}
		case res := <-results:
			for {
				select {
				case ev := <-events:
					if !writeEvent(ev) {
						return
					}
				default:
					resp := TaskResponse{Content: res.content, M3u8URLs: res.m3u8s}
					if req.Debug {
						resp.AllURLs = res.allURLs
					}
					if req.Debug || req.IncludeHeaders {
						resp.M3u8Headers = collectM3U8Headers(ctx, res.m3u8s)
					}
					if res.err != nil {
						resp.Error = res.err.Error()
					}
					writeEvent(StreamEvent{Type: "done", Response: &resp})
					return
				}
			}
		case <-r.Context().Done():
			return
		}
	}
}

func handleExtensionCapture(captureHub *extensionCaptureHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var ev extensionCaptureEvent
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		captureHub.capture(ev)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleExtensionCommand(jobHub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		job, ok := jobHub.next()
		if !ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}
}

func handleExtensionResult(jobHub *extensionJobHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var result extensionJobResult
		if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if !jobHub.complete(result) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// stealthScript patches the properties that anti-bot scripts check before our
// page JS runs. Covers the most common detection vectors.
const stealthScript = `
(function () {
	// 1. Remove the webdriver flag entirely.
	Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

	// 2. Restore window.chrome so sites think it's a real Chrome/Edge install.
	window.chrome = { runtime: {} };

	// 3. Make navigator.plugins non-empty (headless has 0 plugins).
	Object.defineProperty(navigator, 'plugins', {
		get: () => [1, 2, 3, 4, 5],
	});

	// 4. Realistic language list.
	Object.defineProperty(navigator, 'languages', {
		get: () => ['en-US', 'en'],
	});

	// 5. Fix permission query - automation returns 'denied' for notifications by default.
	const origQuery = window.navigator.permissions.query;
	window.navigator.permissions.query = (parameters) =>
		parameters.name === 'notifications'
			? Promise.resolve({ state: Notification.permission })
			: origQuery(parameters);
})();
`

const maxResponseBodyScanBytes = 2 * 1024 * 1024

var mediaURLPattern = regexp.MustCompile(`(?i)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*(?:\.m3u8|m3u8|\.mpd|/playlist|/master|manifest)[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*`)

type responseMeta struct {
	url               string
	resourceType      network.ResourceType
	mime              string
	encodedDataLength float64
}

type responseBodyJob struct {
	ctx       context.Context
	requestID network.RequestID
	baseURL   string
}

type responseBodyScanner struct {
	mu        sync.Mutex
	responses map[network.RequestID]responseMeta
	jobs      []responseBodyJob
}

func newResponseBodyScanner() *responseBodyScanner {
	return &responseBodyScanner{
		responses: make(map[network.RequestID]responseMeta),
	}
}

func (s *responseBodyScanner) listen(ctx context.Context, trackURL func(string), trackText func(string, string)) func(interface{}) {
	return func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventRequestWillBeSent:
			if e.Request != nil {
				trackURL(e.Request.URL)
				s.remember(e.RequestID, responseMeta{
					url:          e.Request.URL,
					resourceType: e.Type,
				})
			}
			if e.RedirectResponse != nil {
				trackURL(e.RedirectResponse.URL)
			}
			if e.Initiator != nil {
				trackURL(e.Initiator.URL)
			}
		case *network.EventResponseReceived:
			if e.Response == nil {
				return
			}
			trackURL(e.Response.URL)
			for _, header := range e.Response.Headers {
				if value, ok := header.(string); ok {
					trackText(value, e.Response.URL)
				}
			}
			s.remember(e.RequestID, responseMeta{
				url:          e.Response.URL,
				resourceType: e.Type,
				mime:         e.Response.MimeType,
			})
		case *network.EventLoadingFinished:
			s.finish(ctx, e.RequestID, e.EncodedDataLength)
		case *network.EventWebSocketCreated:
			trackURL(e.URL)
		case *network.EventWebSocketFrameReceived:
			if e.Response != nil {
				trackText(e.Response.PayloadData, "")
			}
		case *network.EventWebSocketFrameSent:
			if e.Response != nil {
				trackText(e.Response.PayloadData, "")
			}
		case *network.EventWebTransportCreated:
			trackURL(e.URL)
		}
	}
}

func (s *responseBodyScanner) remember(requestID network.RequestID, meta responseMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.responses[requestID]
	if meta.url != "" {
		existing.url = meta.url
	}
	if meta.resourceType != "" {
		existing.resourceType = meta.resourceType
	}
	if meta.mime != "" {
		existing.mime = meta.mime
	}
	s.responses[requestID] = existing
}

func (s *responseBodyScanner) finish(ctx context.Context, requestID network.RequestID, encodedDataLength float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, ok := s.responses[requestID]
	if !ok {
		return
	}
	meta.encodedDataLength = encodedDataLength
	delete(s.responses, requestID)

	if !shouldInspectResponseBody(meta) {
		return
	}
	s.jobs = append(s.jobs, responseBodyJob{
		ctx:       ctx,
		requestID: requestID,
		baseURL:   meta.url,
	})
}

func (s *responseBodyScanner) drain() []responseBodyJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := s.jobs
	s.jobs = nil
	return jobs
}

func scrapeExtension(ctx context.Context, req TaskRequest, captureHub *extensionCaptureHub, jobHub *extensionJobHub, browserPath, extensionDir string, emit func(StreamEvent)) (string, []string, []string, error) {
	var (
		m3u8URLs []string
		allURLs  []string
		mu       sync.Mutex
	)

	trackURL := func(u string) {
		u = normalizeCapturedURL(u, req.URL)
		if u == "" {
			return
		}
		mu.Lock()
		allURLs = append(allURLs, u)
		isMedia := isMediaCandidateURL(u)
		if isMedia {
			m3u8URLs = append(m3u8URLs, u)
		}
		mu.Unlock()
		if emit != nil {
			emit(StreamEvent{Type: "url", URL: u, IsMedia: isMedia})
		}
	}
	trackText := func(text, baseURL string) {
		for _, u := range extractCandidateURLs(text, baseURL) {
			trackURL(u)
		}
	}

	jobID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), nextExtensionJobID.Add(1))
	browser, err := launchExtensionBrowser(browserPath, extensionDir, jobID)
	if err != nil {
		return "", nil, nil, err
	}
	defer closeExtensionBrowser(browser)

	unregisterExtensionCapture := captureHub.register(jobID, req.URL, trackURL)
	defer unregisterExtensionCapture()

	resultCh := jobHub.enqueue(&extensionJob{
		JobID:        jobID,
		URL:          req.URL,
		WaitMs:       req.WaitMs,
		LocalStorage: req.LocalStorage,
		Actions:      req.Actions,
		CloseTab:     true,
	})

	var result extensionJobResult
	select {
	case result = <-resultCh:
	case <-ctx.Done():
		return "", dedupe(m3u8URLs), dedupe(allURLs), ctx.Err()
	}

	trackText(result.Content, req.URL)

	mu.Lock()
	m3u8Snapshot := append([]string(nil), m3u8URLs...)
	allSnapshot := append([]string(nil), allURLs...)
	mu.Unlock()

	if result.Error != "" {
		return result.Content, dedupe(m3u8Snapshot), dedupe(allSnapshot), errors.New(result.Error)
	}
	return result.Content, dedupe(m3u8Snapshot), dedupe(allSnapshot), nil
}

func scrapeCDP(ctx, allocCtx context.Context, req TaskRequest, captureHub *extensionCaptureHub, emit func(StreamEvent)) (string, []string, []string, error) {
	// Apply the deadline first so the listener and Run share the exact same context.
	tabCtx, tabCancel := chromedp.NewContext(allocCtx)
	defer tabCancel()

	tabCtx, deadlineCancel := context.WithTimeout(tabCtx, requestTimeout)
	defer deadlineCancel()

	var (
		m3u8URLs []string
		allURLs  []string
		mu       sync.Mutex
	)
	bodyScanner := newResponseBodyScanner()

	// trackURL is the shared URL capture function used by all targets.
	trackURL := func(u string) {
		u = normalizeCapturedURL(u, req.URL)
		if u == "" {
			return
		}
		mu.Lock()
		allURLs = append(allURLs, u)
		isMedia := isMediaCandidateURL(u)
		if isMedia {
			m3u8URLs = append(m3u8URLs, u)
		}
		mu.Unlock()
		if emit != nil {
			emit(StreamEvent{Type: "url", URL: u, IsMedia: isMedia})
		}
	}

	trackText := func(text, baseURL string) {
		for _, u := range extractCandidateURLs(text, baseURL) {
			trackURL(u)
		}
	}

	unregisterExtensionCapture := captureHub.register("", req.URL, trackURL)
	defer unregisterExtensionCapture()

	// attachNetworkListener creates a child context for a discovered target
	// (iframe, popup, service worker) and starts capturing its network events.
	var attachNetworkListener func(parentCtx context.Context, targetID target.ID)
	attachNetworkListener = func(parentCtx context.Context, targetID target.ID) {
		childCtx, _ := chromedp.NewContext(parentCtx,
			chromedp.WithTargetID(targetID),
		)
		childNetworkHandler := bodyScanner.listen(childCtx, trackURL, trackText)
		chromedp.ListenTarget(childCtx, func(ev interface{}) {
			childNetworkHandler(ev)
			switch e := ev.(type) {
			case *target.EventAttachedToTarget:
				go attachNetworkListener(childCtx, e.TargetInfo.TargetID)
			}
		})
		go chromedp.Run(childCtx,
			target.SetAutoAttach(true, false).WithFlatten(true),
			network.Enable().
				WithMaxTotalBufferSize(100*1024*1024).
				WithMaxResourceBufferSize(maxResponseBodyScanBytes).
				WithMaxPostDataSize(1024*1024),
		)
	}

	// Main tab listener - handles network events and discovers child targets.
	mainNetworkHandler := bodyScanner.listen(tabCtx, trackURL, trackText)
	chromedp.ListenTarget(tabCtx, func(ev interface{}) {
		mainNetworkHandler(ev)
		switch e := ev.(type) {
		case *target.EventAttachedToTarget:
			t := string(e.TargetInfo.Type)
			// Capture iframes, popups, AND service workers; all can fetch m3u8.
			if t == "iframe" || t == "page" || t == "service_worker" || t == "worker" {
				go attachNetworkListener(tabCtx, e.TargetInfo.TargetID)
			}
		}
	})

	actions := []chromedp.Action{
		// Flatten=true: child target (iframe) network events flow into the parent session.
		target.SetAutoAttach(true, false).WithFlatten(true),
		network.Enable().
			WithMaxTotalBufferSize(100 * 1024 * 1024).
			WithMaxResourceBufferSize(maxResponseBodyScanBytes).
			WithMaxPostDataSize(1024 * 1024),
		// Inject stealth patches before ANY page JS runs.
		// This removes the webdriver flag and adds missing browser properties
		// that anti-bot scripts check for.
		chromedp.ActionFunc(func(ctx context.Context) error {
			_, err := page.AddScriptToEvaluateOnNewDocument(stealthScript).Do(ctx)
			return err
		}),

		chromedp.Navigate(req.URL),
	}

	// Inject localStorage keys before the page runs its JS, then reload.
	if len(req.LocalStorage) > 0 {
		actions = append(actions, chromedp.ActionFunc(func(ctx context.Context) error {
			for k, v := range req.LocalStorage {
				km, _ := json.Marshal(k)
				vm, _ := json.Marshal(v)
				script := fmt.Sprintf("window.localStorage.setItem(%s, %s);", string(km), string(vm))
				if err := chromedp.Evaluate(script, nil).Do(ctx); err != nil {
					return err
				}
			}
			return nil
		}))
		actions = append(actions, chromedp.Reload())
	}

	var htmlContent string
	actions = append(actions,
		chromedp.WaitReady("body", chromedp.ByQuery),
	)
	for _, reqAction := range req.Actions {
		actions = append(actions, buildBrowserAction(reqAction))
	}
	actions = append(actions,
		chromedp.Sleep(time.Duration(req.WaitMs)*time.Millisecond),
		chromedp.OuterHTML("html", &htmlContent),
	)

	runErr := chromedp.Run(tabCtx, actions...)

	trackText(htmlContent, req.URL)
	scanResponseBodies(bodyScanner, trackText)

	mu.Lock()
	m3u8Snapshot := append([]string(nil), m3u8URLs...)
	allSnapshot := append([]string(nil), allURLs...)
	mu.Unlock()

	if runErr != nil {
		return "", dedupe(m3u8Snapshot), dedupe(allSnapshot), fmt.Errorf("chromedp: %w", runErr)
	}

	return htmlContent, dedupe(m3u8Snapshot), dedupe(allSnapshot), nil
}

func buildBrowserAction(action BrowserAction) chromedp.Action {
	switch strings.ToLower(action.Type) {
	case "wait", "sleep":
		return chromedp.Sleep(time.Duration(action.WaitMs) * time.Millisecond)
	case "click":
		if action.Selector != "" {
			return chromedp.Click(action.Selector, chromedp.ByQuery)
		}
		return chromedp.MouseClickXY(action.X, action.Y)
	case "double_click":
		if action.Selector != "" {
			return chromedp.DoubleClick(action.Selector, chromedp.ByQuery)
		}
		return chromedp.MouseClickXY(action.X, action.Y, chromedp.ClickCount(2))
	case "evaluate", "eval":
		return chromedp.Evaluate(action.Script, nil)
	case "scroll":
		return chromedp.Evaluate(fmt.Sprintf(`window.scrollBy(%f, %f);`, action.DeltaX, action.DeltaY), nil)
	case "send_keys", "type":
		return chromedp.SendKeys(action.Selector, action.Text, chromedp.ByQuery)
	case "wait_ready":
		return chromedp.WaitReady(action.Selector, chromedp.ByQuery)
	default:
		return chromedp.ActionFunc(func(context.Context) error {
			return fmt.Errorf("unsupported action type %q", action.Type)
		})
	}
}

// dedupe removes duplicate strings while preserving order.
func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func shouldInspectResponseBody(meta responseMeta) bool {
	if meta.url != "" && isMediaCandidateURL(meta.url) {
		return true
	}
	if meta.encodedDataLength > maxResponseBodyScanBytes {
		return false
	}

	mime := strings.ToLower(meta.mime)
	if strings.Contains(mime, "json") ||
		strings.Contains(mime, "javascript") ||
		strings.Contains(mime, "mpegurl") ||
		strings.Contains(mime, "dash+xml") ||
		strings.Contains(mime, "text") ||
		strings.Contains(mime, "html") ||
		strings.Contains(mime, "xml") {
		return true
	}

	switch meta.resourceType {
	case network.ResourceTypeDocument,
		network.ResourceTypeScript,
		network.ResourceTypeXHR,
		network.ResourceTypeFetch,
		network.ResourceTypeManifest,
		network.ResourceTypeMedia:
		return true
	default:
		return false
	}
}

func scanResponseBodies(scanner *responseBodyScanner, trackText func(string, string)) {
	deadline := time.Now().Add(6 * time.Second)

	for {
		jobs := scanner.drain()
		if len(jobs) == 0 || time.Now().After(deadline) {
			return
		}

		for _, job := range jobs {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return
			}

			bodyCtx, cancel := context.WithTimeout(job.ctx, remaining)
			var body []byte
			err := chromedp.Run(bodyCtx, chromedp.ActionFunc(func(ctx context.Context) error {
				var err error
				body, err = network.GetResponseBody(job.requestID).Do(ctx)
				return err
			}))
			cancel()

			if err == nil && len(body) > 0 {
				trackText(string(body), job.baseURL)
			}
		}
	}
}

func extractCandidateURLs(text, baseURL string) []string {
	if text == "" {
		return nil
	}

	text = unescapeURLText(text)
	matches := mediaURLPattern.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}

	out := make([]string, 0, len(matches))
	for _, match := range matches {
		u := normalizeCapturedURL(match, baseURL)
		if u != "" {
			out = append(out, u)
		}
	}
	return dedupe(out)
}

func normalizeCapturedURL(raw, baseURL string) string {
	raw = strings.TrimSpace(unescapeURLText(raw))
	if raw == "" {
		return ""
	}

	raw = strings.Trim(raw, " \t\r\n\"'<>`),;")
	raw = strings.TrimRight(raw, ".")
	if raw == "" || raw == "about:blank" {
		return ""
	}

	if strings.HasPrefix(raw, "//") {
		if base, err := url.Parse(baseURL); err == nil && base.Scheme != "" {
			raw = base.Scheme + ":" + raw
		} else {
			raw = "https:" + raw
		}
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if parsed.Scheme == "" && baseURL != "" {
		base, err := url.Parse(baseURL)
		if err == nil {
			parsed = base.ResolveReference(parsed)
		}
	}
	if parsed.Scheme == "http" || parsed.Scheme == "https" || parsed.Scheme == "ws" || parsed.Scheme == "wss" {
		return parsed.String()
	}
	if parsed.Scheme == "" {
		return parsed.String()
	}
	return ""
}

func sameCapturedURL(a, b string) bool {
	a = normalizeCapturedURL(a, "")
	b = normalizeCapturedURL(b, "")
	if a == "" || b == "" {
		return false
	}

	parsedA, errA := url.Parse(a)
	parsedB, errB := url.Parse(b)
	if errA != nil || errB != nil {
		return a == b
	}

	parsedA.Fragment = ""
	parsedB.Fragment = ""
	return strings.EqualFold(parsedA.Scheme, parsedB.Scheme) &&
		strings.EqualFold(parsedA.Host, parsedB.Host) &&
		parsedA.EscapedPath() == parsedB.EscapedPath() &&
		parsedA.RawQuery == parsedB.RawQuery
}

func unescapeURLText(text string) string {
	replacer := strings.NewReplacer(
		`\/`, "/",
		`\u0026`, "&",
		`\u002F`, "/",
		`\u003d`, "=",
		`\u003D`, "=",
		`\u003f`, "?",
		`\u003F`, "?",
		`\u003a`, ":",
		`\u003A`, ":",
		"&amp;", "&",
	)
	return replacer.Replace(text)
}

func isMediaCandidateURL(raw string) bool {
	u := strings.ToLower(unescapeURLText(raw))
	return strings.Contains(u, "m3u8") ||
		strings.Contains(u, ".mpd") ||
		strings.Contains(u, "/playlist") ||
		strings.Contains(u, "/master") ||
		strings.Contains(u, "manifest") ||
		strings.Contains(u, ".mp4")
}

func isM3U8URL(raw string) bool {
	return strings.Contains(strings.ToLower(unescapeURLText(raw)), ".m3u8")
}

func collectM3U8Headers(ctx context.Context, mediaURLs []string) []CapturedURLHeader {
	unique := dedupe(mediaURLs)
	filtered := make([]string, 0, len(unique))
	for _, u := range unique {
		if isM3U8URL(u) {
			filtered = append(filtered, u)
			if len(filtered) >= maxHeaderProbes {
				break
			}
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	client := &http.Client{Timeout: headerProbeTimeout}
	out := make([]CapturedURLHeader, 0, len(filtered))
	for _, u := range filtered {
		entry := CapturedURLHeader{URL: u}
		status, method, headers, err := probeHeaders(ctx, client, u)
		if err != nil {
			entry.Error = err.Error()
			out = append(out, entry)
			continue
		}

		entry.Status = status
		entry.Method = method
		entry.Headers = headers
		out = append(out, entry)
	}
	return out
}

func probeHeaders(ctx context.Context, client *http.Client, rawURL string) (int, string, map[string]string, error) {
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return 0, "", nil, err
	}
	headReq.Header.Set("User-Agent", "ShadoWare/1.0")

	headResp, err := client.Do(headReq)
	if err == nil && headResp != nil {
		headers := flattenHeaders(headResp.Header)
		status := headResp.StatusCode
		_ = headResp.Body.Close()
		if status != http.StatusMethodNotAllowed && status != http.StatusNotImplemented {
			return status, http.MethodHead, headers, nil
		}
	}

	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, "", nil, err
	}
	getReq.Header.Set("Range", "bytes=0-0")
	getReq.Header.Set("User-Agent", "ShadoWare/1.0")

	getResp, err := client.Do(getReq)
	if err != nil {
		if headResp != nil {
			return 0, http.MethodHead, nil, err
		}
		return 0, http.MethodGet, nil, err
	}
	defer getResp.Body.Close()

	return getResp.StatusCode, http.MethodGet, flattenHeaders(getResp.Header), nil
}

func flattenHeaders(in http.Header) map[string]string {
	out := make(map[string]string, len(in))
	for k, values := range in {
		out[k] = strings.Join(values, ", ")
	}
	return out
}

// startParentWatchdog polls the parent process every 2 seconds.
func startParentWatchdog(shutdown func(reason string)) {
	ppid := os.Getppid()
	log.Printf("Watchdog: monitoring parent PID %d", ppid)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if isParentDead(ppid) {
				shutdown("parent process is gone")
				return
			}
		}
	}()
}

// findBrowser scans common install paths for Edge, Brave, and Chrome.
func findBrowser() (path, name string) {
	type candidate struct {
		name  string
		paths []string
	}

	var browsers []candidate

	switch runtime.GOOS {
	case "windows":
		browsers = []candidate{
			{"Microsoft Edge", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe`),
				os.ExpandEnv(`${ProgramFiles}\Microsoft\Edge\Application\msedge.exe`),
			}},
			{"Brave", []string{
				os.ExpandEnv(`${ProgramFiles}\BraveSoftware\Brave-Browser\Application\brave.exe`),
				os.ExpandEnv(`${LocalAppData}\BraveSoftware\Brave-Browser\Application\brave.exe`),
			}},
			{"Google Chrome", []string{
				os.ExpandEnv(`${ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${ProgramFiles}\Google\Chrome\Application\chrome.exe`),
				os.ExpandEnv(`${LocalAppData}\Google\Chrome\Application\chrome.exe`),
			}},
		}
	case "darwin":
		browsers = []candidate{
			{"Microsoft Edge", []string{"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"}},
			{"Brave", []string{"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"}},
			{"Google Chrome", []string{"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}},
		}
	default: // Linux
		browsers = []candidate{
			{"Microsoft Edge", []string{"/usr/bin/microsoft-edge", "/usr/bin/microsoft-edge-stable"}},
			{"Brave", []string{"/usr/bin/brave-browser", "/usr/bin/brave"}},
			{"Google Chrome", []string{
				"/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
				"/usr/bin/chromium", "/usr/bin/chromium-browser",
			}},
		}
	}

	for _, b := range browsers {
		for _, p := range b.paths {
			if _, err := os.Stat(p); err == nil {
				return p, b.name
			}
		}
	}
	return "", ""
}

func validateRequest(req TaskRequest) error {
	if req.URL == "" {
		return errors.New("url is required")
	}
	parsed, err := url.ParseRequestURI(req.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("url must be a valid http/https URL")
	}
	if req.WaitMs < 0 {
		return errors.New("wait_ms must be non-negative")
	}
	if req.WaitMs > 15_000 {
		return errors.New("wait_ms cannot exceed 15000 (15 seconds)")
	}
	for i, action := range req.Actions {
		if err := validateBrowserAction(action); err != nil {
			return fmt.Errorf("actions[%d]: %w", i, err)
		}
	}
	return nil
}

func validateBrowserAction(action BrowserAction) error {
	switch strings.ToLower(action.Type) {
	case "wait", "sleep":
		if action.WaitMs < 0 || action.WaitMs > 30_000 {
			return errors.New("wait_ms must be between 0 and 30000")
		}
	case "click", "double_click":
		if action.Selector == "" && action.X == 0 && action.Y == 0 {
			return errors.New("click requires selector or x/y")
		}
	case "evaluate", "eval":
		if strings.TrimSpace(action.Script) == "" {
			return errors.New("evaluate requires script")
		}
	case "scroll":
		if action.DeltaX == 0 && action.DeltaY == 0 {
			return errors.New("scroll requires delta_x or delta_y")
		}
	case "send_keys", "type":
		if action.Selector == "" {
			return errors.New("send_keys requires selector")
		}
	case "wait_ready":
		if action.Selector == "" {
			return errors.New("wait_ready requires selector")
		}
	default:
		return fmt.Errorf("unsupported type %q", action.Type)
	}
	return nil
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func writeError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(TaskResponse{Error: msg})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/extension-") {
			return
		}
		log.Printf("%s %s - %s", r.Method, r.URL.Path, time.Since(start))
	})
}
