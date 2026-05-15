# MCP Server for AI Agents

This directory contains a minimal **MCP (Model Context Protocol)** server that
exposes Apache Pulsar produce / consume / peek operations as tools for AI
agents such as [Cursor](https://cursor.com) and
[Claude Desktop](https://claude.ai/download).

The server communicates over **stdio** (line-delimited JSON-RPC 2.0) — no extra
PHP dependencies are needed beyond the existing `pulsar-client-php` library.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| PHP >= 7.1 | Same as the library itself |
| Composer dependencies installed | `composer install` in the project root |
| A running Apache Pulsar broker | Default `pulsar://localhost:6650` |

Quick way to start a local Pulsar broker for testing:

```bash
docker run -it --rm -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
```

---

## Quick Start (manual smoke test)

```bash
# From the project root
printf '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1.0"}}}\n{"jsonrpc":"2.0","method":"notifications/initialized"}\n{"jsonrpc":"2.0","id":2,"method":"tools/list"}\n' \
  | php examples/mcp-server.php
```

You should see two JSON responses on stdout (one for `initialize`, one for
`tools/list`) and log lines on stderr.

---

## Cursor IDE Configuration

Add the following to your Cursor MCP config (`~/.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "pulsar": {
      "command": "php",
      "args": ["/absolute/path/to/pulsar-client-php/examples/mcp-server.php"],
      "env": {
        "PULSAR_BROKER_URL": "pulsar://localhost:6650",
        "PULSAR_TOKEN": ""
      }
    }
  }
}
```

> Replace `/absolute/path/to` with the real path on your machine.
> Leave `PULSAR_TOKEN` empty if no JWT authentication is required.

After saving, restart Cursor — the three Pulsar tools will appear in the agent
tool list.

---

## Claude Desktop Configuration

Add to `claude_desktop_config.json`
(macOS `~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "pulsar": {
      "command": "php",
      "args": ["/absolute/path/to/pulsar-client-php/examples/mcp-server.php"],
      "env": {
        "PULSAR_BROKER_URL": "pulsar://localhost:6650",
        "PULSAR_TOKEN": ""
      }
    }
  }
}
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PULSAR_BROKER_URL` | `pulsar://localhost:6650` | Pulsar broker URL (`pulsar://`, `pulsar+ssl://`, `http://`, `https://`) |
| `PULSAR_TOKEN` | *(empty)* | JWT token for authentication. Leave empty to skip auth. |

---

## Tools Reference

### `pulsar_publish`

Publish a message to a Pulsar topic.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `topic` | string | **yes** | — | Full topic, e.g. `persistent://public/default/my-topic` |
| `payload` | string | **yes** | — | Message body |
| `key` | string | no | — | Routing key (for partitioned topics) |
| `properties` | object | no | — | Key-value string properties |
| `delay_seconds` | integer | no | — | Delay delivery (needs Shared / Key\_Shared subscription) |

**Example call:**

```json
{
  "jsonrpc": "2.0", "id": 10, "method": "tools/call",
  "params": {
    "name": "pulsar_publish",
    "arguments": {
      "topic": "persistent://public/default/demo",
      "payload": "{\"hello\":\"world\"}",
      "properties": {"source": "mcp-agent"}
    }
  }
}
```

**Example response:**

```json
{
  "jsonrpc": "2.0", "id": 10,
  "result": {
    "content": [{"type": "text", "text": "{\n    \"message_id\": \"621:103:0\"\n}"}]
  }
}
```

---

### `pulsar_consume`

Consume messages from a topic (auto-acknowledged).

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `topic` | string | **yes** | — | Full topic name |
| `subscription` | string | **yes** | — | Subscription name |
| `subscription_type` | string | no | `Shared` | `Exclusive` / `Shared` / `Failover` / `Key_Shared` |
| `max_messages` | integer | no | `10` | Max messages to return |
| `timeout_seconds` | integer | no | `3` | Max wait time in seconds |

**Example call:**

```json
{
  "jsonrpc": "2.0", "id": 11, "method": "tools/call",
  "params": {
    "name": "pulsar_consume",
    "arguments": {
      "topic": "persistent://public/default/demo",
      "subscription": "mcp-sub",
      "max_messages": 5
    }
  }
}
```

**Example response:**

```json
{
  "jsonrpc": "2.0", "id": 11,
  "result": {
    "content": [{"type": "text", "text": "{\n    \"messages\": [...],\n    \"count\": 3\n}"}]
  }
}
```

Each message object contains: `message_id`, `topic`, `publish_time`, `key`,
`properties`, `payload`.

---

### `pulsar_peek`

Read messages using a Reader — **read-only**, no subscription side effects, no
acknowledgement.

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `topic` | string | **yes** | — | Full topic name |
| `from` | string | no | `earliest` | `earliest` or `latest` |
| `max_messages` | integer | no | `10` | Max messages to return |
| `timeout_seconds` | integer | no | `2` | Max wait time in seconds |

> **Note:** When reading from `earliest` on a topic with existing messages,
> results are returned immediately. Reading from `latest` waits for *new*
> messages and a single internal read may block for up to 30 seconds if no
> messages arrive.

**Example call:**

```json
{
  "jsonrpc": "2.0", "id": 12, "method": "tools/call",
  "params": {
    "name": "pulsar_peek",
    "arguments": {
      "topic": "persistent://public/default/demo",
      "from": "earliest",
      "max_messages": 3
    }
  }
}
```

---

## Next Steps (not yet implemented)

- Stateful consumer sessions (open / receive / ack / close with handles)
- TLS & Basic auth support
- Schema-aware produce/consume
- HTTP/SSE transport
- MCP Resources & Prompts
