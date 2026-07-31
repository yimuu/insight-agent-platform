package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	protocol = "2026-07-28"
	tasksExt = "io.modelcontextprotocol/tasks"
	sdk      = "github.com/modelcontextprotocol/go-sdk@91e4e1a0b8ca01cfa680f142815b1152a0513326"
)

type request struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type response struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      any            `json:"id"`
	Result  map[string]any `json:"result,omitempty"`
	Error   map[string]any `json:"error,omitempty"`
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

func require(condition bool, message string) error {
	if !condition {
		return errors.New(message)
	}
	return nil
}

func modernMeta() map[string]any {
	return map[string]any{
		"io.modelcontextprotocol/protocolVersion": protocol,
		"io.modelcontextprotocol/clientInfo": map[string]any{
			"name": "insight-go-qualification", "version": "1.0.0",
		},
		"io.modelcontextprotocol/clientCapabilities": map[string]any{
			"extensions": map[string]any{tasksExt: map[string]any{}},
		},
	}
}

func decodeRequest(data []byte) (*request, error) {
	if _, err := jsonrpc.DecodeMessage(data); err != nil {
		return nil, fmt.Errorf("official SDK rejected JSON-RPC: %w", err)
	}
	var value request
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	if value.JSONRPC != "2.0" || value.Method == "" || value.ID == nil {
		return nil, errors.New("invalid JSON-RPC request")
	}
	meta, ok := value.Params["_meta"].(map[string]any)
	if !ok || meta["io.modelcontextprotocol/protocolVersion"] != protocol {
		return nil, errors.New("missing modern protocol metadata")
	}
	return &value, nil
}

func sdkJSON(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var mapped map[string]any
	if err := json.Unmarshal(data, &mapped); err != nil {
		return nil, err
	}
	return mapped, nil
}

func task(status string) map[string]any {
	return map[string]any{
		"taskId":         "go-task-1",
		"status":         status,
		"createdAt":      "2026-07-30T00:00:00Z",
		"lastUpdatedAt":  "2026-07-30T00:00:01Z",
		"ttlMs":          60000,
		"pollIntervalMs": 25,
	}
}

func resultFor(value *request) (response, error) {
	var result map[string]any
	switch value.Method {
	case "server/discover":
		result = map[string]any{
			"resultType":        "complete",
			"supportedVersions": []string{protocol},
			"capabilities": map[string]any{
				"tools":      map[string]any{},
				"extensions": map[string]any{tasksExt: map[string]any{}},
			},
			"serverInfo": map[string]any{"name": "go-reference-fixture", "version": sdk},
			"ttlMs":      1000,
			"cacheScope": "private",
		}
	case "tools/list":
		echo := &mcp.Tool{
			Name:        "sdk_echo",
			Description: "Echo through the pinned Go SDK fixture.",
			InputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"value": map[string]any{"type": "string"}},
				"required":             []string{"value"},
				"additionalProperties": false,
			},
			OutputSchema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"value": map[string]any{"type": "string"}},
				"required":             []string{"value"},
				"additionalProperties": false,
			},
		}
		taskTool := &mcp.Tool{
			Name:        "sdk_task",
			Description: "Create a task through the pinned Go SDK fixture.",
			InputSchema: map[string]any{"type": "object", "additionalProperties": false},
		}
		listed, err := sdkJSON(&mcp.ListToolsResult{Tools: []*mcp.Tool{echo, taskTool}})
		if err != nil {
			return response{}, err
		}
		listed["resultType"] = "complete"
		listed["ttlMs"] = 1000
		listed["cacheScope"] = "private"
		result = listed
	case "tools/call":
		name, _ := value.Params["name"].(string)
		if name == "sdk_task" {
			result = task("working")
			result["resultType"] = "task"
		} else {
			arguments, _ := value.Params["arguments"].(map[string]any)
			text, _ := arguments["value"].(string)
			called, err := sdkJSON(&mcp.CallToolResult{
				Content:           []mcp.Content{&mcp.TextContent{Text: text}},
				StructuredContent: map[string]any{"value": text},
			})
			if err != nil {
				return response{}, err
			}
			called["resultType"] = "complete"
			called["isError"] = false
			result = called
		}
	case "tasks/get":
		if value.Params["taskId"] != "go-task-1" {
			return response{}, errors.New("unexpected task id")
		}
		result = task("completed")
		result["resultType"] = "complete"
		result["result"] = map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "go-task-complete"}},
			"isError": false,
		}
	case "tasks/update", "tasks/cancel":
		if value.Params["taskId"] != "go-task-1" {
			return response{}, errors.New("unexpected task id")
		}
		result = map[string]any{"resultType": "complete"}
	default:
		return response{
			JSONRPC: "2.0",
			ID:      value.ID,
			Error:   map[string]any{"code": -32601, "message": "Method not found"},
		}, nil
	}
	return response{JSONRPC: "2.0", ID: value.ID, Result: result}, nil
}

func handle(data []byte) ([]byte, error) {
	req, err := decodeRequest(data)
	if err != nil {
		return nil, err
	}
	res, err := resultFor(req)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	if _, err := jsonrpc.DecodeMessage(encoded); err != nil {
		return nil, fmt.Errorf("official SDK rejected response JSON-RPC: %w", err)
	}
	return encoded, nil
}

func serveStdio() error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)
	writer := bufio.NewWriter(os.Stdout)
	for scanner.Scan() {
		data, err := handle(scanner.Bytes())
		if err != nil {
			return err
		}
		if _, err := writer.Write(append(data, '\n')); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func serveHTTP(port string) error {
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(writer http.ResponseWriter, req *http.Request) {
		body, readErr := io.ReadAll(io.LimitReader(req.Body, 16*1024*1024))
		if readErr != nil {
			http.Error(writer, "invalid body", http.StatusBadRequest)
			return
		}
		var parsed request
		if json.Unmarshal(body, &parsed) != nil ||
			req.Method != http.MethodPost ||
			req.Header.Get("Mcp-Protocol-Version") != protocol ||
			req.Header.Get("Mcp-Method") != parsed.Method {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		encoded, handleErr := handle(body)
		if handleErr != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_, _ = writer.Write(encoded)
	})
	fmt.Printf("{\"ready\":\"http://%s/mcp\",\"sdk\":%q}\n", listener.Addr().String(), sdk)
	return http.Serve(listener, mux)
}

var nextID int

func callHTTP(endpoint, method string, params map[string]any) (map[string]any, error) {
	nextID++
	params["_meta"] = modernMeta()
	wire := request{
		JSONRPC: "2.0",
		ID:      "go-" + strconv.Itoa(nextID),
		Method:  method,
		Params:  params,
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	if _, err := jsonrpc.DecodeMessage(body); err != nil {
		return nil, fmt.Errorf("official SDK rejected client request: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", protocol)
	req.Header.Set("Mcp-Method", method)
	req.Header.Set("Authorization", "Bearer qualification-secret")
	if name, ok := params["name"].(string); ok {
		req.Header.Set("Mcp-Name", name)
	}
	if taskID, ok := params["taskId"].(string); ok {
		req.Header.Set("Mcp-Name", taskID)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("platform HTTP status %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 16*1024*1024))
	if err != nil {
		return nil, err
	}
	if _, err := jsonrpc.DecodeMessage(data); err != nil {
		return nil, fmt.Errorf("official SDK rejected platform response: %w", err)
	}
	var decoded response
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	if decoded.Error != nil {
		return nil, fmt.Errorf("platform protocol error: %v", decoded.Error)
	}
	return decoded.Result, nil
}

func runClientHTTP(endpoint string) error {
	discover, err := callHTTP(endpoint, "server/discover", map[string]any{})
	if err != nil {
		return err
	}
	versions, _ := discover["supportedVersions"].([]any)
	if err := require(len(versions) == 1 && versions[0] == protocol, "modern version not advertised"); err != nil {
		return err
	}
	listed, err := callHTTP(endpoint, "tools/list", map[string]any{})
	if err != nil {
		return err
	}
	tools, _ := listed["tools"].([]any)
	if err := require(len(tools) >= 2, "platform exports missing"); err != nil {
		return err
	}
	complete, err := callHTTP(endpoint, "tools/call", map[string]any{
		"name": "qualified_echo", "arguments": map[string]any{"value": "go-client"},
	})
	if err != nil {
		return err
	}
	structured, _ := complete["structuredContent"].(map[string]any)
	if err := require(structured["value"] == "go-client", "tool result mismatch"); err != nil {
		return err
	}
	created, err := callHTTP(endpoint, "tools/call", map[string]any{
		"name": "qualified_task", "arguments": map[string]any{},
	})
	if err != nil {
		return err
	}
	taskID, _ := created["taskId"].(string)
	completed, err := callHTTP(endpoint, "tasks/get", map[string]any{"taskId": taskID})
	if err != nil {
		return err
	}
	if err := require(completed["status"] == "completed", "task did not complete"); err != nil {
		return err
	}
	fmt.Printf("{\"qualified\":true,\"sdk\":%q,\"transport\":\"streamable_http\",\"tasks\":true}\n", sdk)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: go-interop server-stdio|server-http|client-http [endpoint|port]")
	}
	var err error
	switch os.Args[1] {
	case "server-stdio":
		err = serveStdio()
	case "server-http":
		port := "0"
		if len(os.Args) > 2 {
			port = strings.TrimSpace(os.Args[2])
		}
		err = serveHTTP(port)
	case "client-http":
		if len(os.Args) < 3 {
			fail("missing platform endpoint")
		}
		err = runClientHTTP(os.Args[2])
	default:
		err = errors.New("unknown command")
	}
	if err != nil {
		fail(err.Error())
	}
}
