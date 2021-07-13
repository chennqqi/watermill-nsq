package nsq_test

import (
	"os"
	"testing"

	"github.com/chennqqi/watermill-nsq/pkg/nsq"
	stdnsq "github.com/nsqio/go-nsq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/stretchr/testify/require"
)

func newPubSub(t *testing.T, clientID string, queueName string) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, true)

	nsqdURL := os.Getenv("WATERMILL_TEST_NSQD_URL")
	if nsqdURL == "" {
		nsqdURL = "localhost:4150"
	}
	nsqLookupURL := os.Getenv("WATERMILL_TEST_NSQLOOKUPD_URL")
	if nsqLookupURL == "" {
		nsqLookupURL = "localhost:4161"
	}

	config := stdnsq.NewConfig()
	config.MaxAttempts = 1000

	pub, err := nsq.NewNsqPublisher(
		nsq.NsqPublisherConfig{
			NsqD:   nsqdURL,
			Config: config,
		}, logger)
	require.NoError(t, err)

	sub, err := nsq.NewNsqSubscriber(
		nsq.NsqSubscriberConfig{
			GroupName: queueName,
			Config:       config,
			NsqdAddrs: []string{nsqdURL,},
			LookupdAddrs: []string{nsqLookupURL},
		}, logger)

	require.NoError(t, err)
	return pub, sub
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, watermill.NewUUID(), "test-queue")
}

func createPubSubWithDurable(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, consumerGroup, consumerGroup)
}

func TestPublishSubscribe(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithDurable,
	)
}
