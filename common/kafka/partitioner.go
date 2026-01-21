package kafka

import (
	"math"
	"strings"

	"github.com/IBM/sarama"
)

// LexicographicalPartitioner distributes messages based on the alphabetical order of keys.
// It maps keys to a continuous range [0.0, 1.0) and splits them evenly across available partitions.
type LexicographicalPartitioner struct {
	defaultPartitioner sarama.Partitioner
}

// NewLexicographicalPartitioner initializes a new partitioner instance.
func NewLexicographicalPartitioner(topic string) sarama.Partitioner {
	return &LexicographicalPartitioner{
		defaultPartitioner: sarama.NewHashPartitioner(topic),
	}
}

// Partition determines the partition ID for a given message.
// It scales the lexicographical position of the key to the number of available partitions.
func (p *LexicographicalPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	keyBytes, err := msg.Key.Encode()
	if err != nil || len(keyBytes) == 0 {
		return p.defaultPartitioner.Partition(msg, numPartitions)
	}
	symbol := strings.ToUpper(string(keyBytes))

	// Calculate the relative position of the symbol in the alphabetical space [0.0, 1.0).
	position := calculateStringPosition(symbol)

	// Map the position to a specific partition index
	partition := int32(math.Floor(position * float64(numPartitions)))

	// Boundary check to ensure the index is within valid range [0, numPartitions-1].
	if partition >= numPartitions {
		partition = numPartitions - 1
	}

	return partition, nil
}

// RequiresConsistency indicates that this partitioner ensures the same key always maps to the same partition
// (provided the number of partitions remains constant).
func (p *LexicographicalPartitioner) RequiresConsistency() bool {
	return true
}

// calculateStringPosition converts a string into a float64 value in the range [0.0, 1.0).
// It treats the string as a number in base-26 system, where 'A'=0 and 'Z'=25.
func calculateStringPosition(s string) float64 {
	var score float64 = 0.0
	var weight float64 = 1.0 / 26.0

	// Limit precision to avoid floating point issues and unnecessary computation.
	const precisionLimit = 5
	limit := len(s)
	if limit > precisionLimit {
		limit = precisionLimit
	}

	for i := 0; i < limit; i++ {
		char := s[i]

		// Normalize character to 0-25 range.
		val := 0.0
		if char >= 'A' && char <= 'Z' {
			val = float64(char - 'A')
		} else if char >= 'a' && char <= 'z' {
			val = float64(char - 'a')
		}

		// Accumulate value: value * (1/26^position)
		score += val * weight
		weight /= 26.0
	}

	return score
}
