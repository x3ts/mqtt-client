<?php

namespace x3ts\mqtt\client;

use Swoole\Client as SwClient;
use x3ts\mqtt\client\exceptions\ConnectException;
use x3ts\mqtt\client\exceptions\InvalidResponseException;
use x3ts\mqtt\client\exceptions\IOException;
use x3ts\mqtt\protocol\constants\QoS;
use x3ts\mqtt\protocol\messages\ConnAck;
use x3ts\mqtt\protocol\messages\Connect;
use x3ts\mqtt\protocol\messages\MessageBase;
use x3ts\mqtt\protocol\messages\PubAck;
use x3ts\mqtt\protocol\messages\PubComp;
use x3ts\mqtt\protocol\messages\Publish;
use x3ts\mqtt\protocol\messages\PubRec;
use x3ts\mqtt\protocol\messages\PubRel;
use x3ts\mqtt\protocol\messages\SubAck;
use x3ts\mqtt\protocol\messages\Subscribe;
use x3ts\mqtt\protocol\messages\Will;

class Client
{
    protected string $buffer = '';

    protected SwClient $client;

    public function __construct(
        protected string $host = 'localhost',
        protected int $port = 1883,
        protected float $connectTimeout = 5,
    ) {
        $this->client = new SwClient(SWOOLE_TCP | SWOOLE_KEEP);
        $this->client->set([
            'open_mqtt_protocol' => true,
            'open_tcp_nodelay'   => true,
            'package_max_length' => 1024 * 1024 * 8,
        ]);
    }

    protected function openTcpConnection(): bool
    {
        if (!$this->client->isConnected()) {
            $ok = $this->client->connect($this->host, $this->port, $this->connectTimeout);
            if (!$ok) {
                throw new IOException(
                    'connect to mqtt server failed: ' . socket_strerror($this->client->errCode),
                    $this->client->errCode,
                );
            }
            return true;
        }
        return false;
    }

    /**
     * @throws ConnectException
     * @throws InvalidResponseException
     * @throws IOException
     */
    public function reconnect(): static
    {
        if ($this->openTcpConnection()) {
            /** @var ConnAck $r */
            $r = $this->sendConnect($this->connectPacket->setCleanSession(false));
            if ($r->sessionPresent) {
                $this->resendPublishingPackets();
            }
        }
        return $this;
    }

    public function resendPublishingPackets(): void
    {
        foreach ($this->publishingPackets as $publish) {
            assert($publish instanceof Publish);
            $this->send($publish->setDup(true));
        }
    }

    protected array $publishingPackets = [];

    /**
     * @throws IOException
     */
    public function recv(): MessageBase
    {
        $buffer = $this->client->recv(0, SwClient::MSG_DONTWAIT);
        $msg = MessageBase::decode($buffer);

        if ($msg instanceof Publish) {
            if ($msg->qos === QoS::AT_LEAST_ONCE) {
                $this->send(PubAck::newInstance()->setPacketIdentifier($msg->packetIdentifier));
            } else if ($msg->qos === QoS::EXACTLY_ONCE) {
                $this->qos2Packets[$msg->packetIdentifier] = $msg;
                $this->send(PubRec::newInstance()->setPacketIdentifier($msg->packetIdentifier));
            }
            $this->receivedMessages[] = $msg;
            return $msg;
        }
        if ($msg instanceof PubAck) {
            if (isset($this->publishingPackets[$msg->packetIdentifier])) {
                unset($this->publishingPackets[$msg->packetIdentifier]);
            }
            return $msg;
        }
        if ($msg instanceof PubRec) {
            if (isset($this->publishingPackets[$msg->packetIdentifier])) {
                $this->send(PubRel::newInstance()->setPacketIdentifier($msg->packetIdentifier));
            }
        }
        if ($msg instanceof PubRel) {
            if (isset($this->qos2Packets[$msg->packetIdentifier])) {
                $this->send(PubComp::newInstance()->setPacketIdentifier($msg->packetIdentifier));
                $this->receivedMessages[] = $this->qos2Packets[$msg->packetIdentifier];
                unset($this->qos2Packets[$msg->packetIdentifier]);
            }
            return $msg;
        }
        if ($msg instanceof PubComp) {
            if (isset($this->publishingPackets[$msg->packetIdentifier])) {
                unset($this->publishingPackets[$msg->packetIdentifier]);
            }
            return $msg;
        }

        $this->receivedMessages[] = $msg;
        return $msg;
    }

    protected array $qos2Packets = [];

    /**
     * @throws IOException
     */
    protected function nextMessage(): MessageBase
    {
        while (count($this->receivedMessages) === 0) {
            $this->recv();
        }
        return array_shift($this->receivedMessages);
    }

    public const EV_CONNECT = 'connect';
    public const EV_CONNECTED = 'connected';

    protected array $_eventHandlers = [];

    public function on(string $event, callable $handler): static
    {
        if (!array_key_exists($event, $this->_eventHandlers)) {
            $this->_eventHandlers[$event] = [];
        }
        $this->_eventHandlers[$event][] = $handler;
        return $this;
    }

    protected array $_once = [];

    public function once(string $event, callable $handler): static
    {
        if (!array_key_exists($event, $this->_once)) {
            $this->_once[$event] = [];
        }
        $this->_once[$event][] = $handler;
        return $this;
    }

    public function dispatch(string $event, ...$args): static
    {
        if (array_key_exists($event, $this->_eventHandlers)) {
            foreach ($this->_eventHandlers as $handler) {
                $handler(...$args);
            }
        }
        if (array_key_exists($event, $this->_once)) {
            foreach ($this->_once[$event] as $key => $handler) {
                $handler(...$args);
                unset($this->_once[$event][$key]);
            }
        }
        return $this;
    }

    public function disconnect()
    {
    }

    private Connect $connectPacket;

    public function connect(
        string $clientId,
        string $username = '',
        string $password = '',
        bool $cleanSession = true,
        ?Will $will = null
    ): ConnAck {
        $msg = Connect::newInstance()
            ->setClientIdentifier($clientId)
            ->setUsername($username)
            ->setPassword($password)
            ->setCleanSession($cleanSession);
        if ($will) {
            $msg->setWill($will);
        }
        $this->connectPacket = $msg;
        $this->client->connect($this->host, $this->port, $this->connectTimeout);
        return $this->sendConnect($msg);
    }

    protected function sendConnect(Connect $msg)
    {
        $this->receivedMessages = [];
        $this->dispatch(self::EV_CONNECT, $msg);
        $this->send($msg);

        $resp = $this->nextMessage();
        if (!$resp instanceof ConnAck) {
            throw new InvalidResponseException('expecting ConnAck response');
        }
        if ($resp->ackCode !== ConnAck::Accepted) {
            throw new ConnectException('connect to mqtt server failed. server response: ' . $resp->ackCode);
        }
        $this->dispatch(self::EV_CONNECTED, $resp);
        return $resp;
    }

    protected array $receivedMessages = [];

    protected array $subscribedTopics = [];

    /**
     * @throws IOException
     */
    public function subscribe(string|array $topic, int $maximumQoS = 0)
    {
        $msg = Subscribe::newInstance()
            ->genPacketIdentifier();
        if (is_array($topic)) {
            foreach ($topic as [$topicFilter, $maxQoS]) {
                $msg->addTopicFilter($topicFilter, $maxQoS);
            }
        } else {
            $msg->addTopicFilter($topic, $maximumQoS);
        }
        $this->send($msg);
        $subAck = $this->nextMessage();
        if ($subAck instanceof SubAck && $subAck->packetIdentifier === $msg->packetIdentifier) {
            $i = 0;
            foreach ($msg->topicQosPairs as [$topicFilter, $qos]) {
                $confirmQoS = $subAck->returnCodes[$i];
                if ($confirmQoS !== 0x80) {
                    $this->subscribedTopics[$topicFilter] = $confirmQoS;
                }
            }
        }
    }

    public function send(MessageBase $msg): static
    {
        $packet = $msg->encode();
        $r = $this->client->send($packet);
        if ($r === false) {
            throw new IOException(
                'writing underlying stream failed: ' . socket_strerror($this->client->errCode),
                $this->client->errCode
            );
        }

        return $this;
    }
}
