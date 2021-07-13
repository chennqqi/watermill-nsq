package nsq

import (
	stdnsq "github.com/nsqio/go-nsq"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type NsqPublisherConfig struct {
	NsqD string

	// nsq config
	*stdnsq.Config

	// Marshaler is marshaler used to marshal messages to stan format.
	Marshaler Marshaler
}

func (c NsqPublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("NsqPublisherConfig.Marshaler is missing")
	}

	return nil
}

type NsqPublisher struct {
	config NsqPublisherConfig
	logger watermill.LoggerAdapter

	producer *stdnsq.Producer
}

// NewNsqPublisher creates a new NsqPublisher.
func NewNsqPublisher(config NsqPublisherConfig, logger watermill.LoggerAdapter) (*NsqPublisher, error) {
	producer, err := stdnsq.NewProducer(config.NsqD, config.Config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}
	if config.Marshaler == nil {
		config.Marshaler = &GobMarshaler{}
	}

	return &NsqPublisher{
		producer: producer,
		config:   config,
		logger:   logger,
	}, nil

}

// Publish publishes message to NATS.
//
// Publish will not return until an ack has been received from NATS Streaming.
// When one of messages delivery fails - function is interrupted.
func (p NsqPublisher) Publish(topic string, messages ...*message.Message) error {
	var nsqMsgIndex int
	nsqMessages := make([][]byte, len(messages))
	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)
		b, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}
		nsqMessages[nsqMsgIndex] = b
		nsqMsgIndex++
	}
	if err := p.producer.MultiPublish(topic, nsqMessages); err != nil {
		return errors.Wrap(err, "sending message failed")
	}
	return nil
}

func (p NsqPublisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("NsqPublisher closed", nil)

	p.producer.Stop()
	return nil
}
