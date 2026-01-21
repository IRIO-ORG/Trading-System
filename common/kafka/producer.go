package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// ProtoProducer wraps sarama.SyncProducer to simplify sending Protobuf messages.
type ProtoProducer struct {
	internal sarama.SyncProducer
}

// NewProtoProducerWithLexographicalPartitioner creates a new
// wrapped producer using the LexicographicalPartitioner.
func NewProtoProducerWithLexographicalPartitioner() (*ProtoProducer, error) {
	saramaProd, err := NewProducer(NewLexicographicalPartitioner)
	if err != nil {
		return nil, err
	}
	return &ProtoProducer{internal: saramaProd}, nil
}

// NewProtoProducer creates a new wrapped producer using the factory defined in `factory.go`.
func NewProtoProducer() (*ProtoProducer, error) {
	saramaProd, err := NewProducer(sarama.NewHashPartitioner)
	if err != nil {
		return nil, err
	}
	return &ProtoProducer{internal: saramaProd}, nil
}

func (p *ProtoProducer) Close() error {
	return p.internal.Close()
}

// Send serializes a protobuf message and sends it to the specified topic.
// key: Used for partitioning (e.g., Symbol). If empty, RoundRobin is used.
// msg: The protobuf struct (must implement proto.Message).
func (p *ProtoProducer) Send(topic string, key string, msg proto.Message) error {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal proto message: %w", err)
	}

	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}

	// Set key if provided
	if key != "" {
		kafkaMsg.Key = sarama.StringEncoder(key)
	}

	// Blocking - waits for ACKs based on factory config (WaitForAll)
	// We ignore partition and offset.
	_, _, err = p.internal.SendMessage(kafkaMsg)
	if err != nil {
		return fmt.Errorf("failed to produce message to %s (key=%s): %w", topic, key, err)
	}

	return nil
}
