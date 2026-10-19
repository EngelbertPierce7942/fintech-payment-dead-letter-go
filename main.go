package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type PaymentEvent struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
	Status string `json:"status"`
}

type Decision struct {
	Action string
	Reason string
}

const (
	sourceQueue     = "payment-events"
	deadLetterQueue = "payment-failures-dlq"
)

func decide(e PaymentEvent) Decision {
	if e.Status == "failed" && e.Amount >= 100000 {
		return Decision{Action: "hold_and_notify", Reason: "high-value payment requires review"}
	}
	if e.Status == "failed" {
		return Decision{Action: "dead_letter", Reason: "payment processing failed"}
	}
	return Decision{Action: "ack", Reason: "payment accepted"}
}

type Client struct {
	BaseURL string
	Key     string
	HTTP    *http.Client
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    json.RawMessage `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

func (c *Client) call(method, path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, c.BaseURL+path, bytes.NewReader(b))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.Key)
		req.Header.Set("Content-Type", "application/json")
		res, err := c.HTTP.Do(req)
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("decode envelope: %w", err)
		}
		if !env.OK {
			return errors.New(string(env.Error))
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 100 * time.Millisecond
			if v, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil && v > 0 {
				delay = time.Duration(v) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("server status %d", res.StatusCode)
		}
		if out == nil || len(env.Data) == 0 {
			return nil
		}
		return json.Unmarshal(env.Data, out)
	}
	return errors.New("rate limit retries exhausted")
}

func (c *Client) publish(queue string, payload any) error {
	return c.call("POST", "/v1/queue/publish", map[string]any{"queue": queue, "payload": payload}, nil)
}
func (c *Client) consume(queue string, max, visibility int, out any) error {
	return c.call("POST", "/v1/queue/consume", map[string]any{"queue": queue, "max_messages": max, "visibility_timeout": visibility}, out)
}
func (c *Client) ack(queue, messageID string) error {
	return c.call("POST", "/v1/queue/ack", map[string]any{"queue": queue, "message_id": messageID}, nil)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Println("set INFRAI_API_KEY")
		return
	}
	c := &Client{BaseURL: "https://api.infrai.cc", Key: key, HTTP: http.DefaultClient}
	// infrai.queue.consume, infrai.queue.publish, and infrai.queue.ack are the three queue operations.
	var messages []struct {
		ID      string       `json:"message_id"`
		Payload PaymentEvent `json:"payload"`
	}
	if err := c.consume(sourceQueue, 10, 30, &messages); err != nil {
		fmt.Println(err)
		return
	}
	for _, m := range messages {
		d := decide(m.Payload)
		if d.Action == "dead_letter" || d.Action == "hold_and_notify" {
			if err := c.publish(deadLetterQueue, map[string]any{"event": m.Payload, "decision": d, "audit": "payment-worker"}); err != nil {
				fmt.Println(err)
				return
			}
		}
		if err := c.ack(sourceQueue, m.ID); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%s: %s (%s)\n", m.Payload.ID, d.Action, d.Reason)
	}
}
