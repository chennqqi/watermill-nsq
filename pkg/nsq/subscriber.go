package nsq

import (
	"context"
	"sync"
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type NsqSubscriberConfig struct {
	// nsq config
	// nsq increse MaxAttempts to avoid TestResendOnError, see github.com/ThreeDotsLabs/watermill/pubsub/tests/test_pubsub.go L54
	*nsq.Config
	LookupdAddrs []string
	NsqdAddrs    []string
	GroupName string

	// CloseTimeout determines how long subscriber will wait for Ack/Nack on close.
	// When no Ack/Nack is received after CloseTimeout, subscriber will be closed.
	CloseTimeout time.Duration

	// How long subscriber should wait for Ack/Nack. When no Ack/Nack was received, message will be redelivered.
	// It is mapped to stan.AckWait option.
	AckWaitTimeout time.Duration

	// Requeue delay, default 1s
	//
	RequeueTimeout time.Duration

	// Unmarshaler is an unmarshaler used to unmarshaling messages from NATS format to Watermill format.
	Unmarshaler Unmarshaler
}

func (c *NsqSubscriberConfig) setDefaults() {
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = time.Second * 30
	}
	if c.AckWaitTimeout <= 0 {
		c.AckWaitTimeout = time.Second * 30
	}
	if c.RequeueTimeout <= 0 {
		c.RequeueTimeout = time.Second
	}

	c.Unmarshaler = &GobMarshaler{}
}

func (c *NsqSubscriberConfig) Validate() error {
	if c.Unmarshaler == nil {
		return errors.New("NsqSubscriberConfig.Unmarshaler is missing")
	}

	if len(c.NsqdAddrs) == 0 && len(c.LookupdAddrs) == 0 {
		return errors.New(
			"Either nsqdaddrs or lookupdaddrs should be set",
		)
	}
	return nil
}

type NsqSubscriber struct {
	logger watermill.LoggerAdapter
	config NsqSubscriberConfig

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed bool
}

func NewNsqSubscriber(config NsqSubscriberConfig, logger watermill.LoggerAdapter) (*NsqSubscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &NsqSubscriber{
		logger:  logger,
		config:  config,
		closing: make(chan struct{}),
	}, nil
}

// Subscribe subscribes messages from Nsq topic.
// 	context.WithValue(ctx, "channel", "")
func (s *NsqSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.closed {
		return nil, errors.New("subscriber closed")
	}
	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider": "nsq",
		"topic":    topic,
	}
	channel, ok := ctx.Value("channel").(string)
	if !ok {
		logFields["channel"] = channel
		s.logger.Info("Subcriber not set channel, use configed group name", logFields)
		channel = s.config.GroupName
	}
	requeueTo, ok := ctx.Value("requeue_timeout").(time.Duration)
	if !ok {
		s.logger.Info("Subcriber not set requeue_timeout, use config instand", logFields)
		requeueTo = s.config.RequeueTimeout
	}

	s.logger.Info("Subscribing to Nsq topic", logFields)
	output := make(chan *message.Message, 0)
	consumer, err := nsq.NewConsumer(topic, channel, s.config.Config)
	if err != nil {
		s.subscribersWg.Done()
		s.logger.Error("Cannot NewConsumer", err, logFields)
		return nil, err
	}

	consumer.AddHandler(&nsqMessageHandler{
		ctx:        ctx,
		consumer:   consumer,
		requeueTo:  requeueTo,
		config:     &s.config,
		closing:    s.closing,
		outputChan: output,
		logger:     s.logger,
		topic:      topic,
		channel:    channel,
	})

	// start consuming
	if len(s.config.NsqdAddrs) > 0 {
		err = consumer.ConnectToNSQDs(s.config.NsqdAddrs)
		logFields["nsq_nsqds"] = s.config.NsqdAddrs
	} else if len(s.config.LookupdAddrs) > 0 {
		err = consumer.ConnectToNSQLookupds(s.config.LookupdAddrs)
		logFields["nsq_lookupds"] = s.config.LookupdAddrs
	}
	if err != nil {
		s.logger.Error("Cannot connect", err, logFields)
		s.subscribersWg.Done()
		close(output)
		return nil, err
	}

	// close
	go func(subscriber *nsq.Consumer, subscriberLogFields watermill.LogFields) {
		select {
		case <-ctx.Done():
			s.logger.Trace("Context Done, before stoping consumer", logFields)
		case <-s.closing:
			s.logger.Trace("Closing, before stoping consumer", logFields)
		}
		consumer.Stop()
		s.logger.Trace("Closing, after stop consumer", logFields)
		<-consumer.StopChan
		s.logger.Trace("Closing, stopped consumer", logFields)
		close(output)
		s.subscribersWg.Done()
	}(consumer, logFields)
	return output, nil
}

func (s *NsqSubscriber) Close() error {
	if s.closed {
		return nil
	}
	s.logger.Debug("Closing subscriber", nil)
	defer s.logger.Info("NsqSubscriber closed", nil)

	s.closed = true
	close(s.closing)
	s.subscribersWg.Wait()

	return nil
}

type nsqMessageHandler struct {
	ctx        context.Context
	requeueTo  time.Duration
	consumer   *nsq.Consumer
	config     *NsqSubscriberConfig
	closing    chan struct{}
	outputChan chan<- *message.Message
	logger     watermill.LoggerAdapter
	topic      string
	channel    string
}

func (s *nsqMessageHandler) HandleMessage(m *nsq.Message) error {
	logFields := watermill.LogFields{
		"provider":       "nsq",
		"consumer_group": s.channel,
		"topic":          s.topic,
		"nsq_message_id": fmt.Sprintf("%s", m.ID),
	}

	s.processMessage(s.ctx, m, s.outputChan, logFields)
	return nil
}

func (s *nsqMessageHandler) processMessage(
	ctx context.Context,
	m *nsq.Message,
	output chan<- *message.Message,
	logFields watermill.LogFields,
) {

	s.logger.Trace("Received message", logFields)
	msg, err := s.config.Unmarshaler.Unmarshal(m)
	if err != nil {
		s.logger.Error("Cannot unmarshal message", err, logFields)
		return
	}
	m.DisableAutoResponse()

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	messageLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", messageLogFields)

	select {
	case output <- msg:
		s.logger.Trace("Message sent to consumer", messageLogFields)
	case <-s.closing:
		m.RequeueWithoutBackoff(s.requeueTo)
		s.logger.Trace("Closing, message discarded", messageLogFields)
		return
	case <-ctx.Done():
		m.RequeueWithoutBackoff(s.requeueTo)
		s.logger.Trace("Context cancelled, message discarded", messageLogFields)
		return
	}

	select {
	case <-msg.Acked():
		m.Finish()
		s.logger.Trace("Message Acked", messageLogFields)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", messageLogFields)
		//m.Touch()
		m.RequeueWithoutBackoff(s.requeueTo)
		return
	case <-time.After(s.config.AckWaitTimeout):
		s.logger.Trace("Ack timeouted", messageLogFields)
		m.RequeueWithoutBackoff(s.requeueTo)
		return
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", messageLogFields)
		m.RequeueWithoutBackoff(s.requeueTo)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded before ack", messageLogFields)
		m.RequeueWithoutBackoff(s.requeueTo)
		return
	}
}
