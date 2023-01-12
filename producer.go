package main
import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"github.com/riferrei/srclient"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Payment struct {
	ID     string    `json:"id"`
	Amount float32   `json:"amount"`
}

func main() {

	topic := "transactions-value"

	// 1) Fetch the latest version of the schema
	schemaRegistryClient := srclient.CreateSchemaRegistryClient("https://psrc-68gz8.us-east-2.aws.confluent.cloud")
	schemaRegistryClient.SetCredentials(os.Getenv("SR_USER"), os.Getenv("SR_PASSWORD"))
	schema, err := schemaRegistryClient.GetLatestSchema(topic)
	if err != nil {
        panic(fmt.Sprintf("Error getting the schema %s", err))
    }
	log.Println(schema)
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

	// 2) Serialize the record using the schema provided by the client
	newPayment := Payment{ID: "1", Amount: 12.99}
	value, _ := json.Marshal(newPayment)
	native, _, _ := schema.Codec().NativeFromTextual(value)
	valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)


	// 3) Create the producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "pkc-pgq85.us-west-2.aws.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "acks": "all",
        "retries": "0",
        "sasl.username": os.Getenv("SASL_USERNAME"),
        "sasl.password": os.Getenv("SASL_PASSWORD")})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for event := range p.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the message '%s'\n", message.Key)
				} else {
					fmt.Printf("Message '%s' delivered successfully!\n", message.Key)
				}
			}
		}
	}()

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte("0000000000"), Value: recordValue}
		, nil)

	p.Flush(15 * 1000)

}