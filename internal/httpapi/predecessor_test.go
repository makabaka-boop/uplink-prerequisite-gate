package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
)

type commandJSON struct {
	ID            int64   `json:"id"`
	Status        string  `json:"status"`
	PredecessorID *int64  `json:"predecessor_id"`
	BlockedBy     *int64  `json:"blocked_by"`
	BlockedAt     *string `json:"blocked_at"`
}

func createCommandRaw(t *testing.T, tsURL string, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, tsURL+"/commands", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("non-json response %d: %s", resp.StatusCode, raw)
		}
	}
	return resp.StatusCode, m
}

func claimID(t *testing.T, tsURL string, ms int) (int, map[string]any) {
	t.Helper()
	return doJSON(t, http.MethodPost, tsURL+"/claims", map[string]any{"lease_duration_ms": ms})
}

func ackJSON(t *testing.T, tsURL string, id int64, token, result string) (int, map[string]any) {
	t.Helper()
	return doJSON(t, http.MethodPost, tsURL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": token, "result": result})
}

func getCommandJSON(t *testing.T, tsURL string, id int64) map[string]any {
	t.Helper()
	status, m := doJSON(t, http.MethodGet, tsURL+"/commands/"+strconv.FormatInt(id, 10), nil)
	if status != http.StatusOK {
		t.Fatalf("get command %d: %d %v", id, status, m)
	}
	return m
}

// TestPredecessorHTTPContract 验证 HTTP/查询响应与存储使用同一 blocked 状态契约。
func TestPredecessorHTTPContract(t *testing.T) {
	ts := newTestServer(t)

	status, m := createCommandRaw(t, ts.URL, `{"payload":{"seq":1}}`)
	if status != http.StatusCreated {
		t.Fatalf("root: status=%d body=%v", status, m)
	}
	root := commandJSON{ID: int64(m["id"].(float64)), Status: m["status"].(string)}
	if root.Status != "pending" || m["predecessor_id"] != nil {
		t.Fatalf("legacy-shaped root response: %v", m)
	}

	status, m = createCommandRaw(t, ts.URL,
		`{"payload":{"seq":2},"predecessor_id":`+strconv.FormatInt(root.ID, 10)+`}`)
	if status != http.StatusCreated {
		t.Fatalf("child: status=%d body=%v", status, m)
	}
	childID := int64(m["id"].(float64))
	if m["predecessor_id"].(float64) != float64(root.ID) || m["status"] != "pending" {
		t.Fatalf("child response: %v", m)
	}
	if m["blocked_by"] != nil || m["blocked_at"] != nil {
		t.Fatalf("pending child must not expose blocked fields: %v", m)
	}

	status, m = createCommandRaw(t, ts.URL,
		`{"payload":{"seq":3},"predecessor_id":`+strconv.FormatInt(childID, 10)+`}`)
	if status != http.StatusCreated {
		t.Fatalf("grand: status=%d body=%v", status, m)
	}
	grandID := int64(m["id"].(float64))

	status, m = createCommandRaw(t, ts.URL, `{"payload":{"seq":4}}`)
	if status != http.StatusCreated {
		t.Fatalf("independent: status=%d body=%v", status, m)
	}
	independentID := int64(m["id"].(float64))

	if status, _ := claimID(t, ts.URL, 1000); status != http.StatusNoContent {
		t.Fatalf("child claimed before predecessor delivered: %d", status)
	}

	status, m = claimID(t, ts.URL, 2000)
	if status != http.StatusOK || m["command_id"].(float64) != float64(root.ID) {
		t.Fatalf("claim root: %d %v", status, m)
	}
	rootToken := m["lease_token"].(string)
	if status, m = ackJSON(t, ts.URL, root.ID, rootToken, "delivered"); status != http.StatusOK ||
		m["status"] != "delivered" {
		t.Fatalf("deliver root: %d %v", status, m)
	}

	status, m = claimID(t, ts.URL, 2000)
	if status != http.StatusOK || m["command_id"].(float64) != float64(childID) {
		t.Fatalf("claim child: %d %v", status, m)
	}
	if status, _ := claimID(t, ts.URL, 1000); status != http.StatusNoContent {
		t.Fatalf("grand claimed before child delivered: %d", status)
	}
	childToken := m["lease_token"].(string)
	if status, m = ackJSON(t, ts.URL, childID, childToken, "failed"); status != http.StatusOK ||
		m["status"] != "failed" {
		t.Fatalf("fail child: %d %v", status, m)
	}

	// 孙指令应原子转为 blocked，记录阻断来源为直接失败的子指令。
	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands/"+strconv.FormatInt(grandID, 10), nil)
	if status != http.StatusOK || m["status"] != "blocked" {
		t.Fatalf("grand status: %d %v", status, m)
	}
	if m["blocked_by"].(float64) != float64(childID) || m["blocked_at"] == nil {
		t.Fatalf("grand blocking source missing: %v", m)
	}
	if m["settlement"] != nil || m["lease_generation"].(float64) != 0 ||
		len(m["leases"].([]any)) != 0 {
		t.Fatalf("blocked grand must not have lease or settlement: %v", m)
	}

	// blocked 终态不接受伪造确认；状态契约返回 command_blocked。
	if status, m = ackJSON(t, ts.URL, grandID, "never-issued", "delivered"); status != http.StatusConflict {
		t.Fatalf("ack blocked: %d %v", status, m)
	} else if m["error"].(map[string]any)["code"] != "command_blocked" {
		t.Fatalf("blocked ack code = %v", m)
	}
	if status, _ := claimID(t, ts.URL, 1000); status != http.StatusOK {
		t.Fatalf("independent root should remain claimable: %d", status)
	}

	// 失败后再创建后继：201 返回 blocked 终态。
	status, m = createCommandRaw(t, ts.URL,
		`{"payload":{"seq":5},"predecessor_id":`+strconv.FormatInt(childID, 10)+`}`)
	if status != http.StatusCreated || m["status"] != "blocked" {
		t.Fatalf("create after failed predecessor: %d %v", status, m)
	}
	if m["blocked_by"].(float64) != float64(childID) {
		t.Fatalf("late child blocked source: %v", m)
	}

	for _, body := range []string{
		`{"payload":{},"predecessor_id":"1"}`,
		`{"payload":{},"predecessor_id":true}`,
		`{"payload":{},"predecessor_id":0}`,
		`{"payload":{},"predecessor_id":-1}`,
		`{"payload":{},"predecessor_id":1.5}`,
	} {
		if status, m = createCommandRaw(t, ts.URL, body); status != http.StatusUnprocessableEntity {
			t.Fatalf("body %s: status=%d want 422", body, status)
		} else if m["error"].(map[string]any)["code"] != "invalid_predecessor" {
			t.Fatalf("body %s code=%v", body, m)
		}
	}
	if status, m = createCommandRaw(t, ts.URL, `{"payload":{},"predecessor_id":987654321}`); status != http.StatusUnprocessableEntity {
		t.Fatalf("unknown predecessor status=%d want 422", status)
	} else if m["error"].(map[string]any)["code"] != "unknown_predecessor" {
		t.Fatalf("unknown predecessor code=%v", m)
	}

	// 列表与详情使用同一状态字段和阻断信息。
	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands", nil)
	if status != http.StatusOK {
		t.Fatalf("list: %d", status)
	}
	found := false
	for _, raw := range m["commands"].([]any) {
		cmd := raw.(map[string]any)
		if int64(cmd["id"].(float64)) == grandID {
			found = true
			if cmd["status"] != "blocked" || cmd["blocked_by"].(float64) != float64(childID) {
				t.Fatalf("list blocked contract: %v", cmd)
			}
		}
	}
	if !found {
		t.Fatalf("grand not present in list: %v", m)
	}
	_ = independentID
}
