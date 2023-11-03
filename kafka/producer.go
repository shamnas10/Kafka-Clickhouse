package kafka

import (
	"bufio"

	"fmt"

	"os"

	"github.com/IBM/sarama"
)

var Topic = "smpl"

func Producer() {

	// Create Kafka producer configuration

	config := sarama.NewConfig()

	config.Producer.Return.Successes = true

	// Initialize Kafka producer

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)

	if err != nil {

		panic(err)

	}

	defer producer.Close()

	// Read the CSV file and send each line as a Kafka message

	file, err := os.Open("/home/shamnas/Downloads/output.csv")

	if err != nil {

		panic(err)

	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		csvLine := scanner.Text()

		message := &sarama.ProducerMessage{

			Topic: Topic,

			Value: sarama.StringEncoder(csvLine),
		}

		// Use the producer to send the message

		_, _, err := producer.SendMessage(message)

		if err != nil {

			fmt.Printf("Error sending message: %v\n", err)

		} else {

			fmt.Printf("Produced message: %s\n", csvLine)

		}

	}

	if err := scanner.Err(); err != nil {

		panic(err)

	}

}
