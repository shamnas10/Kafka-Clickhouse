package kafka

import (
	"fmt"
	"kafkaclickhouse/clickhousedb"
	"strconv"

	"strings"

	"github.com/IBM/sarama"

	_ "github.com/go-sql-driver/mysql"
)

func Consumer() {
	// Define Kafka consumer configuration.
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config) // Replace with your Kafka broker(s) address.
	if err != nil {
		fmt.Printf("Failed to create Kafka consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	// Define the Kafka topic you want to consume from.
	topic := "smpl"

	// Create a Kafka consumer for the same topic.
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("Failed to create partition consumer: %v\n", err)
		return
	}
	defer partitionConsumer.Close()

	// Consume messages from Kafka and insert into ClickHouse.
	for msg := range partitionConsumer.Messages() {
		message := string(msg.Value)
		fmt.Printf("Received message from Kafka: %s\n", message)

		db, err := clickhousedb.DB()
		if err != nil {
			fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
			continue // Skip this message and proceed to the next Kafka message
		}

		tx, err := db.Begin()
		if err != nil {
			fmt.Printf("Failed to start transaction: %v\n", err)
			db.Close()
			continue
		}

		// Split the CSV message into individual values based on the comma separator.
		csvValues := strings.Split(message, ",")

		// Assuming your ClickHouse table has columns contact_id, campaign_id, activity_type, and date, adjust this code accordingly.
		if len(csvValues) >= 4 {
			contact_idstr := csvValues[0]
			contact_id, err := strconv.Atoi(contact_idstr)
			if err != nil {
				fmt.Printf("Failed to convert contact_id to integer: %v\n", err)
				tx.Rollback()
				db.Close()
				continue // Skip this message and proceed to the next Kafka message
			}

			campaign_idstr := csvValues[1]
			campaign_id, err := strconv.Atoi(campaign_idstr)
			if err != nil {
				fmt.Printf("Failed to convert campaign_id to integer: %v\n", err)
				tx.Rollback()
				db.Close()
				continue // Skip this message and proceed to the next Kafka message
			}

			activity_typestr := csvValues[2]
			activity_type, err := strconv.Atoi(activity_typestr)
			if err != nil {
				fmt.Printf("Failed to convert activity_type to integer: %v\n", err)
				tx.Rollback()
				db.Close()
				continue // Skip this message and proceed to the next Kafka message
			}

			date := csvValues[3]

			// Prepare and execute the SQL INSERT statement.
			_, err = tx.Exec("INSERT INTO sampleone (contact_id, campaign_id, activity_type, date) VALUES (?, ?, ?, ?)",
				contact_id, campaign_id, activity_type, date)

			if err != nil {
				fmt.Printf("Failed to insert data into ClickHouse: %v\n", err)
				tx.Rollback()
			} else {
				fmt.Println("Inserted data into ClickHouse successfully.")
				tx.Commit()
			}
		} else {
			fmt.Println("Invalid CSV format: Insufficient columns.")
		}

		db.Close()
	}
}
