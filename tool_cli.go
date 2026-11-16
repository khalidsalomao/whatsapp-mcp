package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// The CLI adapter uses the same handlers as MCP for local development and
// integrations that invoke commands. Pair/logout remain separate.
var localToolPaths = map[string]string{
	"get_status": "/status", "search_contacts": "/contacts", "list_chats": "/chats",
	"get_messages": "/messages", "get_message": "/message", "search_messages": "/messages",
	"send_message": "/send", "start_monitor": "/monitors/start", "list_monitors": "/monitors/list",
	"stop_monitor": "/monitors/stop", "claim_reply_jobs": "/reply-jobs/claim", "draft_reply": "/reply-jobs/draft",
	"send_reply": "/reply-jobs/send", "skip_reply": "/reply-jobs/skip", "list_reply_jobs": "/reply-jobs/list",
}

func callLocalTool(ctx context.Context, c *localClient, tool string, input io.Reader, output io.Writer) error {
	path, ok := localToolPaths[tool]
	if !ok {
		return errors.New("unknown tool; choose a tool from the README Tools table")
	}
	decoder := json.NewDecoder(io.LimitReader(input, (64<<10)+1))
	var in map[string]json.RawMessage
	if err := decoder.Decode(&in); err != nil || in == nil {
		return errors.New("provide one JSON object on stdin (use {} for an empty input)")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("expected one JSON object")
	}
	for name, required := range map[string]string{"get_messages": "chat_id", "search_messages": "query"} {
		if name == tool && (len(in[required]) == 0 || string(in[required]) == `""` || string(in[required]) == "null") {
			return errors.New(required + " is required")
		}
	}
	method := "POST"
	if tool == "get_status" {
		if len(in) != 0 {
			return errors.New("get_status accepts an empty object")
		}
		method = "GET"
	}
	var out json.RawMessage
	if err := c.call(ctx, method, path, in, &out); err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(out)
}
