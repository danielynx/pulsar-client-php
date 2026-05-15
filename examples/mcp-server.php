#!/usr/bin/env php
<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Pulsar\Authentication\Jwt;
use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\MessageNotFound;
use Pulsar\Message;
use Pulsar\MessageOptions;
use Pulsar\Options;
use Pulsar\Producer;
use Pulsar\ProducerOptions;
use Pulsar\Reader;
use Pulsar\ReaderOptions;
use Pulsar\SubscriptionType;

/**
 * Minimal MCP (Model Context Protocol) server exposing Apache Pulsar
 * produce / consume / peek operations as tools for AI agents.
 *
 * Transport : stdio (line-delimited JSON-RPC 2.0)
 * Env vars  : PULSAR_BROKER_URL (default pulsar://localhost:6650)
 *             PULSAR_TOKEN      (optional JWT token)
 */
class PulsarMcpServer
{
    const PROTOCOL_VERSION = '2024-11-05';
    const SERVER_NAME      = 'pulsar-mcp-server';
    const SERVER_VERSION   = '0.1.0';

    /** @var string */
    private $brokerUrl;

    /** @var string|null */
    private $token;

    public function __construct()
    {
        $this->brokerUrl = getenv('PULSAR_BROKER_URL') ?: 'pulsar://localhost:6650';
        $token = getenv('PULSAR_TOKEN');
        $this->token = ($token !== false && $token !== '') ? $token : null;
    }

    public function run()
    {
        $this->log('Server started, broker=%s', $this->brokerUrl);

        while (($line = fgets(STDIN)) !== false) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }

            $request = json_decode($line, true);
            if (!is_array($request)) {
                continue;
            }

            $response = $this->dispatch($request);
            if ($response !== null) {
                fwrite(STDOUT, json_encode($response, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . "\n");
                fflush(STDOUT);
            }
        }
    }

    // ------------------------------------------------------------------ //
    //  JSON-RPC dispatch
    // ------------------------------------------------------------------ //

    private function dispatch(array $request)
    {
        $method = isset($request['method']) ? $request['method'] : '';
        $id     = isset($request['id']) ? $request['id'] : null;
        $params = isset($request['params']) ? $request['params'] : [];

        $this->log('Received method=%s id=%s', $method, json_encode($id));

        switch ($method) {
            case 'initialize':
                return $this->handleInitialize($id);

            case 'notifications/initialized':
                return null;

            case 'ping':
                return $this->result($id, []);

            case 'tools/list':
                return $this->handleToolsList($id);

            case 'tools/call':
                return $this->handleToolsCall($id, $params);

            default:
                if ($id !== null) {
                    return $this->error($id, -32601, 'Method not found: ' . $method);
                }
                return null;
        }
    }

    // ------------------------------------------------------------------ //
    //  MCP handlers
    // ------------------------------------------------------------------ //

    private function handleInitialize($id)
    {
        return $this->result($id, [
            'protocolVersion' => self::PROTOCOL_VERSION,
            'capabilities'    => [
                'tools' => new \stdClass(),
            ],
            'serverInfo' => [
                'name'    => self::SERVER_NAME,
                'version' => self::SERVER_VERSION,
            ],
        ]);
    }

    private function handleToolsList($id)
    {
        return $this->result($id, [
            'tools' => $this->toolDefinitions(),
        ]);
    }

    private function handleToolsCall($id, array $params)
    {
        $name = isset($params['name']) ? $params['name'] : '';
        $args = isset($params['arguments']) ? $params['arguments'] : [];

        try {
            switch ($name) {
                case 'pulsar_publish':
                    $payload = $this->doPublish($args);
                    break;
                case 'pulsar_consume':
                    $payload = $this->doConsume($args);
                    break;
                case 'pulsar_peek':
                    $payload = $this->doPeek($args);
                    break;
                default:
                    return $this->toolError($id, 'Unknown tool: ' . $name);
            }

            return $this->result($id, [
                'content' => [
                    [
                        'type' => 'text',
                        'text' => json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT),
                    ],
                ],
            ]);
        } catch (\Throwable $e) {
            $this->log('Tool %s error: %s', $name, $e->getMessage());
            return $this->toolError($id, $e->getMessage());
        }
    }

    // ------------------------------------------------------------------ //
    //  Tool: pulsar_publish
    // ------------------------------------------------------------------ //

    private function doPublish(array $args)
    {
        $topic   = $this->requireString($args, 'topic');
        $payload = $this->requireString($args, 'payload');

        $options = new ProducerOptions();
        $options->setConnectTimeout(3);
        $options->setTopic($topic);
        $this->applyAuth($options);

        $producer = new Producer($this->brokerUrl, $options);
        $producer->connect();

        $msgOpts = [];
        if (isset($args['key']) && $args['key'] !== '') {
            $msgOpts[MessageOptions::KEY] = (string)$args['key'];
        }
        if (isset($args['properties']) && is_array($args['properties'])) {
            $msgOpts['properties'] = $args['properties'];
        }
        if (isset($args['delay_seconds']) && (int)$args['delay_seconds'] > 0) {
            $msgOpts[MessageOptions::DELAY_SECONDS] = (int)$args['delay_seconds'];
        }

        $messageId = $producer->send($payload, $msgOpts);
        $producer->close();

        return ['message_id' => $messageId];
    }

    // ------------------------------------------------------------------ //
    //  Tool: pulsar_consume
    // ------------------------------------------------------------------ //

    private function doConsume(array $args)
    {
        $topic        = $this->requireString($args, 'topic');
        $subscription = $this->requireString($args, 'subscription');
        $subTypeName  = isset($args['subscription_type']) ? (string)$args['subscription_type'] : 'Shared';
        $maxMessages  = isset($args['max_messages']) ? max(1, (int)$args['max_messages']) : 10;
        $timeout      = isset($args['timeout_seconds']) ? max(1, (int)$args['timeout_seconds']) : 3;

        $options = new ConsumerOptions();
        $options->setConnectTimeout(3);
        $options->setTopic($topic);
        $options->setSubscription($subscription);
        $options->setSubscriptionType($this->resolveSubType($subTypeName));
        $options->setNackRedeliveryDelay($timeout);
        $options->setReconnectPolicy(false);
        $this->applyAuth($options);

        $consumer = new Consumer($this->brokerUrl, $options);
        $consumer->connect();

        $messages = [];
        $deadline = time() + $timeout;

        for ($i = 0; $i < $maxMessages; $i++) {
            if (time() >= $deadline) {
                break;
            }
            try {
                $message = $consumer->receive(false);
                $messages[] = $this->formatMessage($message);
                $consumer->ack($message);
            } catch (MessageNotFound $e) {
                if ($e->getCode() === MessageNotFound::Ignore) {
                    break;
                }
                throw $e;
            }
        }

        $consumer->close();

        return ['messages' => $messages, 'count' => count($messages)];
    }

    // ------------------------------------------------------------------ //
    //  Tool: pulsar_peek
    // ------------------------------------------------------------------ //

    /**
     * Note: Reader::next() blocks internally (up to 30 s per attempt) until a
     * message arrives.  The wall-clock timeout is enforced *between* successive
     * next() calls but cannot interrupt a single blocking wait.  When reading
     * from "earliest" on a topic that already has messages, the first call
     * normally returns immediately.
     */
    private function doPeek(array $args)
    {
        $topic       = $this->requireString($args, 'topic');
        $from        = isset($args['from']) ? (string)$args['from'] : 'earliest';
        $maxMessages = isset($args['max_messages']) ? max(1, (int)$args['max_messages']) : 10;
        $timeout     = isset($args['timeout_seconds']) ? max(1, (int)$args['timeout_seconds']) : 2;

        $options = new ReaderOptions();
        $options->setConnectTimeout(3);
        $options->setTopic($topic);
        $this->applyAuth($options);

        if ($from === 'latest') {
            $options->setStartMessageID(Message::latestMessageIdData());
        } else {
            $options->setStartMessageID(Message::earliestMessageIdData());
        }

        $reader = new Reader($this->brokerUrl, $options);
        $reader->connect();

        $messages = [];
        $deadline = time() + $timeout;

        for ($i = 0; $i < $maxMessages; $i++) {
            if (time() >= $deadline) {
                break;
            }
            try {
                $message = $reader->next();
                $messages[] = $this->formatMessage($message);
            } catch (\Throwable $e) {
                $this->log('Peek interrupted: %s', $e->getMessage());
                break;
            }
        }

        $reader->close();

        return ['messages' => $messages, 'count' => count($messages)];
    }

    // ------------------------------------------------------------------ //
    //  Helpers
    // ------------------------------------------------------------------ //

    private function formatMessage(Message $message)
    {
        return [
            'message_id'   => $message->getMessageId(),
            'topic'        => $message->getTopic(),
            'publish_time' => $message->getPublishTime(),
            'key'          => $message->getPartitionKey(),
            'properties'   => $message->getProperties(),
            'payload'      => $message->getPayload(),
        ];
    }

    private function resolveSubType($name)
    {
        $map = [
            'Exclusive'  => SubscriptionType::Exclusive,
            'Shared'     => SubscriptionType::Shared,
            'Failover'   => SubscriptionType::Failover,
            'Key_Shared' => SubscriptionType::Key_Shared,
            'KeyShared'  => SubscriptionType::Key_Shared,
        ];
        return isset($map[$name]) ? $map[$name] : SubscriptionType::Shared;
    }

    private function applyAuth(Options $options)
    {
        if ($this->token !== null) {
            $options->setAuthentication(new Jwt($this->token));
        }
    }

    private function requireString(array $args, $key)
    {
        if (!isset($args[$key]) || !is_string($args[$key]) || $args[$key] === '') {
            throw new \InvalidArgumentException(sprintf('Missing required parameter: %s', $key));
        }
        return $args[$key];
    }

    // ------------------------------------------------------------------ //
    //  JSON-RPC envelope helpers
    // ------------------------------------------------------------------ //

    private function result($id, $result)
    {
        return [
            'jsonrpc' => '2.0',
            'id'      => $id,
            'result'  => $result,
        ];
    }

    private function error($id, $code, $message)
    {
        return [
            'jsonrpc' => '2.0',
            'id'      => $id,
            'error'   => [
                'code'    => $code,
                'message' => $message,
            ],
        ];
    }

    private function toolError($id, $message)
    {
        return $this->result($id, [
            'content' => [
                ['type' => 'text', 'text' => 'Error: ' . $message],
            ],
            'isError' => true,
        ]);
    }

    private function log($format)
    {
        $args = func_get_args();
        array_shift($args);
        fwrite(STDERR, sprintf("[%s] %s\n", date('Y-m-d H:i:s'), vsprintf($format, $args)));
    }

    // ------------------------------------------------------------------ //
    //  Tool definitions (JSON Schema)
    // ------------------------------------------------------------------ //

    private function toolDefinitions()
    {
        return [
            [
                'name'        => 'pulsar_publish',
                'description' => 'Publish a message to an Apache Pulsar topic. Returns the message ID.',
                'inputSchema' => [
                    'type'       => 'object',
                    'required'   => ['topic', 'payload'],
                    'properties' => [
                        'topic' => [
                            'type'        => 'string',
                            'description' => 'Full topic name, e.g. persistent://public/default/my-topic',
                        ],
                        'payload' => [
                            'type'        => 'string',
                            'description' => 'Message body (string). For structured data use a JSON-encoded string.',
                        ],
                        'key' => [
                            'type'        => 'string',
                            'description' => 'Optional routing key for partitioned topics.',
                        ],
                        'properties' => [
                            'type'                 => 'object',
                            'description'          => 'Optional key-value properties attached to the message.',
                            'additionalProperties' => ['type' => 'string'],
                        ],
                        'delay_seconds' => [
                            'type'        => 'integer',
                            'description' => 'Delay delivery by N seconds (needs Shared / Key_Shared subscription on consumer).',
                        ],
                    ],
                ],
            ],
            [
                'name'        => 'pulsar_consume',
                'description' => 'Consume (and auto-acknowledge) messages from a Pulsar topic. Returns up to max_messages within the timeout window.',
                'inputSchema' => [
                    'type'       => 'object',
                    'required'   => ['topic', 'subscription'],
                    'properties' => [
                        'topic' => [
                            'type'        => 'string',
                            'description' => 'Full topic name, e.g. persistent://public/default/my-topic',
                        ],
                        'subscription' => [
                            'type'        => 'string',
                            'description' => 'Subscription name.',
                        ],
                        'subscription_type' => [
                            'type'    => 'string',
                            'enum'    => ['Exclusive', 'Shared', 'Failover', 'Key_Shared'],
                            'default' => 'Shared',
                        ],
                        'max_messages' => [
                            'type'    => 'integer',
                            'default' => 10,
                            'description' => 'Max messages to return (default 10).',
                        ],
                        'timeout_seconds' => [
                            'type'    => 'integer',
                            'default' => 3,
                            'description' => 'Max seconds to wait for messages (default 3).',
                        ],
                    ],
                ],
            ],
            [
                'name'        => 'pulsar_peek',
                'description' => 'Peek at messages using a Reader — read-only, no subscription, no acknowledgement. Good for inspecting topic contents.',
                'inputSchema' => [
                    'type'       => 'object',
                    'required'   => ['topic'],
                    'properties' => [
                        'topic' => [
                            'type'        => 'string',
                            'description' => 'Full topic name, e.g. persistent://public/default/my-topic',
                        ],
                        'from' => [
                            'type'    => 'string',
                            'enum'    => ['earliest', 'latest'],
                            'default' => 'earliest',
                            'description' => 'Start position: "earliest" for first message, "latest" for new messages only.',
                        ],
                        'max_messages' => [
                            'type'    => 'integer',
                            'default' => 10,
                            'description' => 'Max messages to return (default 10).',
                        ],
                        'timeout_seconds' => [
                            'type'    => 'integer',
                            'default' => 2,
                            'description' => 'Max seconds to wait for messages (default 2). A single read may block up to 30 s internally if the topic is empty.',
                        ],
                    ],
                ],
            ],
        ];
    }
}

(new PulsarMcpServer())->run();
