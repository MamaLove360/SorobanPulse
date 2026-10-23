// sdk/conformance/runner_go/runner.go
//
// Go SDK conformance runner for SorobanPulse.
// Reads ../scenarios.yaml, executes every scenario against the live server,
// and reports pass/fail in GitHub Actions step-summary format.
//
// Usage:
//
//	CONFORMANCE_BASE_URL=http://localhost:3000 go run runner.go
//
// Exit code: 0 = all pass, 1 = one or more failures.
package main

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ── YAML schema ───────────────────────────────────────────────────────────────

type Assertion struct {
	Op    string      `yaml:"op"`
	Field string      `yaml:"field"`
	Value interface{} `yaml:"value"`
}

type ScenarioExpect struct {
	Status  int               `yaml:"status"`
	Headers map[string]string `yaml:"headers"`
	Body    []Assertion       `yaml:"body"`
}

type Scenario struct {
	ID             string            `yaml:"id"`
	Description    string            `yaml:"description"`
	Method         string            `yaml:"method"`
	Path           string            `yaml:"path"`
	PathTemplate   string            `yaml:"path_template"`
	Query          map[string]string `yaml:"query"`
	Headers        map[string]string `yaml:"headers"`
	Body           interface{}       `yaml:"body"`
	Expect         ScenarioExpect    `yaml:"expect"`
	SSE            bool              `yaml:"sse"`
	SSETimeoutSecs int               `yaml:"sse_timeout_secs"`
	SSEMinEvents   int               `yaml:"sse_min_events"`
	DependsOn      string            `yaml:"depends_on"`

	// Webhook signature verification scenario fields.
	// When VerifyWebhook is true the runner computes an HMAC-SHA256 over
	// WebhookPayload using WebhookSecret and compares it to WebhookSignature.
	VerifyWebhook    bool   `yaml:"verify_webhook"`
	WebhookSecret    string `yaml:"webhook_secret"`
	WebhookPayload   string `yaml:"webhook_payload"`
	WebhookSignature string `yaml:"webhook_signature"`
	WebhookHeader    string `yaml:"webhook_header"`
}

type ScenariosFile struct {
	BaseURLEnv string     `yaml:"base_url_env"`
	Scenarios  []Scenario `yaml:"scenarios"`
}

// ── Result ────────────────────────────────────────────────────────────────────

type Result struct {
	ID          string
	Description string
	Passed      bool
	Error       string
	DurationMs  int64
}

// jsonResult is the normalized result shape shared by the TypeScript, Python
// and Go runners so CI can diff them for cross-SDK parity.
type jsonResult struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Passed      bool   `json:"passed"`
	Error       string `json:"error,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
}

type jsonReport struct {
	SDK        string       `json:"sdk"`
	BaseURL    string       `json:"base_url"`
	Total      int          `json:"total"`
	Passed     int          `json:"passed"`
	Failed     int          `json:"failed"`
	Results    []jsonResult `json:"results"`
}

// ── Shared state (subscription_id from create_subscription) ──────────────────

var state = map[string]string{}

// ── Field path resolver ────────────────────────────────────────────────────────

// resolveField navigates a JSON-decoded interface{} using a path like "data[0].id".
// Returns (value, true) if found, (nil, false) otherwise.
func resolveField(obj interface{}, path string) (interface{}, bool) {
	// Normalise "data[0].id" → "data.0.id"
	re := regexp.MustCompile(`\[(\d+)\]`)
	normalised := re.ReplaceAllString(path, ".$1")
	parts := strings.Split(normalised, ".")
	// Filter empty parts
	filtered := parts[:0]
	for _, p := range parts {
		if p != "" {
			filtered = append(filtered, p)
		}
	}
	parts = filtered

	current := obj
	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[part]
			if !ok {
				return nil, false
			}
			current = val
		case []interface{}:
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(v) {
				return nil, false
			}
			current = v[idx]
		default:
			return nil, false
		}
	}
	return current, true
}

// ── Assertion engine ──────────────────────────────────────────────────────────

func checkAssertion(body interface{}, a Assertion) string {
	val, found := resolveField(body, a.Field)

	switch a.Op {
	case "exists":
		if !found || val == nil {
			return fmt.Sprintf(`field %q does not exist or is null`, a.Field)
		}
		return ""

	case "type":
		if !found {
			return fmt.Sprintf(`field %q does not exist`, a.Field)
		}
		expected := fmt.Sprintf("%v", a.Value)
		var actual string
		switch val.(type) {
		case []interface{}:
			actual = "array"
		case map[string]interface{}:
			actual = "object"
		case string:
			actual = "string"
		case bool:
			actual = "boolean"
		case float64, int, int64:
			actual = "number"
		case nil:
			actual = "null"
		default:
			actual = "unknown"
		}
		if actual != expected {
			return fmt.Sprintf(`field %q expected type %q, got %q`, a.Field, expected, actual)
		}
		return ""

	case "eq":
		if !found {
			return fmt.Sprintf(`field %q does not exist`, a.Field)
		}
		// Compare via JSON round-trip to normalise numeric types
		actualJSON, _ := json.Marshal(val)
		expectedJSON, _ := json.Marshal(a.Value)
		if string(actualJSON) != string(expectedJSON) {
			return fmt.Sprintf(`field %q expected %s, got %s`, a.Field, expectedJSON, actualJSON)
		}
		return ""

	case "gte":
		if !found {
			return fmt.Sprintf(`field %q does not exist`, a.Field)
		}
		n, ok := toFloat64(val)
		if !ok {
			return fmt.Sprintf(`field %q is not a number`, a.Field)
		}
		exp, ok := toFloat64(a.Value)
		if !ok {
			return fmt.Sprintf(`assertion value is not a number`)
		}
		if n < exp {
			return fmt.Sprintf(`field %q expected >= %v, got %v`, a.Field, exp, n)
		}
		return ""

	case "lte":
		if !found {
			return fmt.Sprintf(`field %q does not exist`, a.Field)
		}
		n, ok := toFloat64(val)
		if !ok {
			return fmt.Sprintf(`field %q is not a number`, a.Field)
		}
		exp, ok := toFloat64(a.Value)
		if !ok {
			return fmt.Sprintf(`assertion value is not a number`)
		}
		if n > exp {
			return fmt.Sprintf(`field %q expected <= %v, got %v`, a.Field, exp, n)
		}
		return ""

	case "len_gte":
		if !found {
			return fmt.Sprintf(`field %q does not exist`, a.Field)
		}
		arr, ok := val.([]interface{})
		if !ok {
			return fmt.Sprintf(`field %q is not an array`, a.Field)
		}
		exp, ok := toFloat64(a.Value)
		if !ok {
			return fmt.Sprintf(`assertion value is not a number`)
		}
		if float64(len(arr)) < exp {
			return fmt.Sprintf(`field %q expected array length >= %v, got %d`, a.Field, exp, len(arr))
		}
		return ""
	}

	return fmt.Sprintf(`unknown assertion op %q`, a.Op)
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func buildURL(base, path string, query map[string]string) string {
	u := strings.TrimSuffix(base, "/") + path
	if len(query) > 0 {
		params := url.Values{}
		for k, v := range query {
			params.Set(k, v)
		}
		u += "?" + params.Encode()
	}
	return u
}

func doHTTPRequest(method, rawURL string, headers map[string]string, body interface{}) (int, map[string]string, string, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, "", fmt.Errorf("marshal body: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, rawURL, reqBody)
	if err != nil {
		return 0, nil, "", err
	}
	req.Header.Set("User-Agent", "soroban-pulse-conformance-go/1.0")
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer resp.Body.Close()

	respHeaders := make(map[string]string)
	for k, vv := range resp.Header {
		if len(vv) > 0 {
			respHeaders[strings.ToLower(k)] = vv[0]
		}
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, respHeaders, "", err
	}
	return resp.StatusCode, respHeaders, string(b), nil
}

// collectSSE opens a raw TCP connection, issues an HTTP GET with
// Accept: text/event-stream, and collects data payloads until we
// hit minEvents or the timeout.
func collectSSE(rawURL string, timeoutSecs, minEvents int) (int, map[string]string, []string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return 0, nil, nil, err
	}

	host := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		if parsed.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	conn, err := net.DialTimeout("tcp", host+":"+port, 5*time.Second)
	if err != nil {
		return 0, nil, nil, err
	}
	defer conn.Close()

	pathAndQuery := parsed.Path
	if parsed.RawQuery != "" {
		pathAndQuery += "?" + parsed.RawQuery
	}
	if pathAndQuery == "" {
		pathAndQuery = "/"
	}

	rawReq := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: %s\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\nUser-Agent: soroban-pulse-conformance-go/1.0\r\nConnection: close\r\n\r\n",
		pathAndQuery, host+":"+port,
	)
	if _, err := conn.Write([]byte(rawReq)); err != nil {
		return 0, nil, nil, err
	}

	if err := conn.SetDeadline(time.Now().Add(time.Duration(timeoutSecs+2) * time.Second)); err != nil {
		return 0, nil, nil, err
	}

	reader := bufio.NewReader(conn)

	// Parse HTTP response status line
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return 0, nil, nil, err
	}
	statusCode := 0
	if parts := strings.SplitN(strings.TrimSpace(statusLine), " ", 3); len(parts) >= 2 {
		statusCode, _ = strconv.Atoi(parts[1])
	}

	// Parse headers
	respHeaders := make(map[string]string)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break // end of headers
		}
		if idx := strings.Index(line, ":"); idx > 0 {
			k := strings.ToLower(strings.TrimSpace(line[:idx]))
			v := strings.TrimSpace(line[idx+1:])
			respHeaders[k] = v
		}
	}

	// Set a tighter deadline for body reads
	if err := conn.SetDeadline(time.Now().Add(time.Duration(timeoutSecs) * time.Second)); err != nil {
		return statusCode, respHeaders, nil, err
	}

	// Collect SSE data lines
	var events []string
	for len(events) < minEvents {
		line, err := reader.ReadString('\n')
		if err != nil {
			// Timeout or EOF — stop collecting
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")
			if data != "" && data != "[DONE]" {
				events = append(events, data)
			}
		}
	}

	return statusCode, respHeaders, events, nil
}

// ── Scenario runner ───────────────────────────────────────────────────────────

func runScenario(s Scenario, baseURL string) Result {
	start := time.Now()

	// ── Webhook signature verification scenario ───────────────────────────────
	if s.VerifyWebhook {
		var errors []string
		expected := s.WebhookSignature
		if expected == "" {
			// Compute the expected signature ourselves so the scenario can
			// simply assert that the SDK helper produces the same value.
			expected = computeWebhookSignature(s.WebhookSecret, s.WebhookPayload)
		}
		actual := computeWebhookSignature(s.WebhookSecret, s.WebhookPayload)
		if !hmac.Equal([]byte(actual), []byte(expected)) {
			errors = append(errors, fmt.Sprintf("webhook signature mismatch: expected %q, got %q", expected, actual))
		}
		return Result{
			ID: s.ID, Description: s.Description,
			Passed:     len(errors) == 0,
			Error:      strings.Join(errors, "; "),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// Resolve path
	resolvedPath := s.Path
	if s.PathTemplate != "" {
		re := regexp.MustCompile(`\{(\w+)\}`)
		resolvedPath = re.ReplaceAllStringFunc(s.PathTemplate, func(match string) string {
			key := match[1 : len(match)-1]
			if v, ok := state[key]; ok {
				return v
			}
			return match
		})
	}

	var errors []string

	// ── SSE scenario ──────────────────────────────────────────────────────────
	if s.SSE {
		rawURL := buildURL(baseURL, resolvedPath, s.Query)
		timeoutSecs := s.SSETimeoutSecs
		if timeoutSecs <= 0 {
			timeoutSecs = 10
		}
		minEvents := s.SSEMinEvents
		if minEvents <= 0 {
			minEvents = 1
		}

		status, respHeaders, events, err := collectSSE(rawURL, timeoutSecs, minEvents)
		if err != nil {
			return Result{
				ID: s.ID, Description: s.Description,
				Passed:     false,
				Error:      err.Error(),
				DurationMs: time.Since(start).Milliseconds(),
			}
		}

		if status != s.Expect.Status {
			errors = append(errors, fmt.Sprintf("status: expected %d, got %d", s.Expect.Status, status))
		}
		for k, v := range s.Expect.Headers {
			actual := respHeaders[strings.ToLower(k)]
			if !strings.Contains(actual, v) {
				errors = append(errors, fmt.Sprintf("header %q: expected to include %q, got %q", k, v, actual))
			}
		}
		if len(events) < minEvents {
			errors = append(errors, fmt.Sprintf("SSE: expected at least %d event(s), got %d within %ds", minEvents, len(events), timeoutSecs))
		}

		errStr := strings.Join(errors, "; ")
		return Result{
			ID: s.ID, Description: s.Description,
			Passed:     len(errors) == 0,
			Error:      errStr,
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// ── HTTP scenario ─────────────────────────────────────────────────────────
	rawURL := buildURL(baseURL, resolvedPath, s.Query)
	method := s.Method
	if method == "" {
		method = "GET"
	}

	status, respHeaders, bodyText, err := doHTTPRequest(method, rawURL, s.Headers, s.Body)
	if err != nil {
		return Result{
			ID: s.ID, Description: s.Description,
			Passed:     false,
			Error:      err.Error(),
			DurationMs: time.Since(start).Milliseconds(),
		}
	}

	// Status check
	if status != s.Expect.Status {
		errors = append(errors, fmt.Sprintf("status: expected %d, got %d", s.Expect.Status, status))
	}

	// Header checks
	for k, v := range s.Expect.Headers {
		actual := respHeaders[strings.ToLower(k)]
		if !strings.Contains(actual, v) {
			errors = append(errors, fmt.Sprintf("header %q: expected to include %q, got %q", k, v, actual))
		}
	}

	// Body assertions
	if len(s.Expect.Body) > 0 {
		var parsed interface{}
		decoder := json.NewDecoder(strings.NewReader(bodyText))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			errors = append(errors, fmt.Sprintf("body is not valid JSON: %s", truncate(bodyText, 200)))
		} else {
			for _, assertion := range s.Expect.Body {
				if msg := checkAssertion(parsed, assertion); msg != "" {
					errors = append(errors, msg)
				}
			}

			// Persist subscription_id for delete_subscription
			if s.ID == "create_subscription" && len(errors) == 0 {
				if m, ok := parsed.(map[string]interface{}); ok {
					if id, ok := m["id"].(string); ok {
						state["subscription_id"] = id
					}
				}
			}
		}
	}

	errStr := strings.Join(errors, "; ")
	return Result{
		ID: s.ID, Description: s.Description,
		Passed:     len(errors) == 0,
		Error:      errStr,
		DurationMs: time.Since(start).Milliseconds(),
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// computeWebhookSignature returns the hex-encoded HMAC-SHA256 of payload
// keyed by secret, matching the signature scheme used by the service.
func computeWebhookSignature(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// ── Reporting ─────────────────────────────────────────────────────────────────

func printResult(r Result) {
	icon := "✅"
	if !r.Passed {
		icon = "❌"
	}
	fmt.Printf("%s [%s] %s (%dms)\n", icon, r.ID, r.Description, r.DurationMs)
	if !r.Passed && r.Error != "" {
		fmt.Printf("   └─ %s\n", r.Error)
	}
}

func writeSummary(results []Result) {
	summaryPath := os.Getenv("GITHUB_STEP_SUMMARY")
	if summaryPath == "" {
		return
	}

	passed, failed := 0, 0
	for _, r := range results {
		if r.Passed {
			passed++
		} else {
			failed++
		}
	}
	total := len(results)
	badge := "🟢"
	if failed > 0 {
		badge = "🔴"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n## %s SDK Conformance — Go (%d/%d passed)\n\n", badge, passed, total))
	sb.WriteString("| # | Scenario | Status | Duration | Error |\n")
	sb.WriteString("|---|----------|--------|----------|-------|\n")
	for _, r := range results {
		status := "✅ Pass"
		if !r.Passed {
			status = "❌ Fail"
		}
		errMsg := strings.ReplaceAll(r.Error, "|", "\\|")
		sb.WriteString(fmt.Sprintf("| | `%s` | %s | %dms | %s |\n", r.ID, status, r.DurationMs, errMsg))
	}
	sb.WriteString(fmt.Sprintf("\n**Total:** %d &nbsp;|&nbsp; **Passed:** %d &nbsp;|&nbsp; **Failed:** %d\n", total, passed, failed))

	f, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not write step summary: %v\n", err)
		return
	}
	defer f.Close()
	f.WriteString(sb.String())
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	// Resolve scenarios.yaml relative to this file's directory
	_, thisFile, _, _ := runtime.Caller(0)
	scenariosPath := filepath.Join(filepath.Dir(thisFile), "..", "scenarios.yaml")

	// Fallback: look next to the binary for CI environments
	if _, err := os.Stat(scenariosPath); os.IsNotExist(err) {
		execDir, _ := os.Executable()
		scenariosPath = filepath.Join(filepath.Dir(execDir), "..", "scenarios.yaml")
	}
	// Second fallback: current working directory
	if _, err := os.Stat(scenariosPath); os.IsNotExist(err) {
		scenariosPath = filepath.Join(".", "scenarios.yaml")
	}

	f, err := os.Open(scenariosPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: cannot open scenarios.yaml: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	var sf ScenariosFile
	if err := yaml.NewDecoder(f).Decode(&sf); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: cannot parse scenarios.yaml: %v\n", err)
		os.Exit(1)
	}

	envKey := sf.BaseURLEnv
	if envKey == "" {
		envKey = "CONFORMANCE_BASE_URL"
	}
	baseURL := os.Getenv(envKey)
	if baseURL == "" {
		baseURL = "http://localhost:3000"
	}

	fmt.Printf("\n=== SorobanPulse SDK Conformance — Go ===\n")
	fmt.Printf("Base URL:  %s\n", baseURL)
	fmt.Printf("Scenarios: %d\n\n", len(sf.Scenarios))

	var results []Result
	for _, scenario := range sf.Scenarios {
		r := runScenario(scenario, baseURL)
		results = append(results, r)
		printResult(r)
	}

	passed, failed := 0, 0
	for _, r := range results {
		if r.Passed {
			passed++
		} else {
			failed++
		}
	}

	fmt.Printf("\n--- Summary ---\n")
	fmt.Printf("Total: %d | Passed: %d | Failed: %d\n", len(results), passed, failed)

	writeSummary(results)

	// Emit normalized JSON results so CI can compare across SDKs.
	report := jsonReport{
		SDK:     "go",
		BaseURL: baseURL,
		Total:   len(results),
		Passed:  passed,
		Failed:  failed,
		Results: make([]jsonResult, 0, len(results)),
	}
	for _, r := range results {
		report.Results = append(report.Results, jsonResult{
			ID:          r.ID,
			Description: r.Description,
			Passed:      r.Passed,
			Error:       r.Error,
			DurationMs:  r.DurationMs,
		})
	}

	jsonPath := os.Getenv("CONFORMANCE_RESULTS_PATH")
	if jsonPath == "" {
		jsonPath = filepath.Join(".", "conformance-go.json")
	}
	if b, err := json.MarshalIndent(report, "", "  "); err == nil {
		if err := os.WriteFile(jsonPath, b, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "warn: could not write JSON results: %v\n", err)
		} else {
			fmt.Printf("JSON results written to %s\n", jsonPath)
		}
	}

	if failed > 0 {
		fmt.Fprintf(os.Stderr, "\n%d scenario(s) failed.\n", failed)
		os.Exit(1)
	}
}
