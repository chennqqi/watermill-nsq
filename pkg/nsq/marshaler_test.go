package nsq_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/chennqqi/watermill-nsq/pkg/nsq"
	stdnsq "github.com/nsqio/go-nsq"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestGobMarshaler(t *testing.T) {
	msg := message.NewMessage("1", []byte("zag"))
	msg.Metadata.Set("foo", "bar")

	marshaler := nsq.GobMarshaler{}

	b, err := marshaler.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := marshaler.Unmarshal(stdnsq.NewMessage(stdnsq.MessageID{0}, b))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))

	unmarshaledMsg.Ack()

	select {
	case <-unmarshaledMsg.Acked():
		// ok
	default:
		t.Fatal("ack is not working")
	}
}

func TestGobMarshaler_multiple_messages_async(t *testing.T) {
	marshaler := nsq.GobMarshaler{}

	messagesCount := 1000
	wg := sync.WaitGroup{}
	wg.Add(messagesCount)

	for i := 0; i < messagesCount; i++ {
		go func(msgNum int) {
			defer wg.Done()

			msg := message.NewMessage(fmt.Sprintf("%d", msgNum), nil)

			b, err := marshaler.Marshal("topic", msg)
			require.NoError(t, err)

			unmarshaledMsg, err := marshaler.Unmarshal(
				stdnsq.NewMessage(stdnsq.MessageID{0}, b))
			require.NoError(t, err)

			assert.True(t, msg.Equals(unmarshaledMsg))
		}(i)
	}

	wg.Wait()
}
