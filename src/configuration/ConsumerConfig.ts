import { Consumer } from "kafkajs";
import { KafkaClient } from "./KafkaClient";

/**
 * Consumer Class Configuration.
 */
export class ConsumerConfig {

    // Kafka Client instance.
    private kafkaClient: KafkaClient;

    // Kafka Consumer instance.
    private kafkaConsumer: Consumer;

    /**
     * Constructor class which connect the service to kafka on application startup.
     */
    constructor(groupId: string, topicName: string) {
        this.kafkaClient = new KafkaClient;
        this.initializeConsumer(groupId, topicName);
    }

    public initializeConsumer(groupId: string, topicName: string): void {

        this.kafkaConsumer = this.kafkaClient.kafkaProperties().consumer({ groupId: groupId });

        // Consumer subscribes to a particular topic and reads the message from beginning.
        this.kafkaConsumer.subscribe({ topic: topicName, fromBeginning: true });

        // Run the consumer and process the each message. Batch message can be also processed.
        this.kafkaConsumer.run({

            // Automatic commit offsets is set to false for manual commiting, automatic commiting will slow the process of each message.
            autoCommit: false,
            // Process the each message. here the topic message is printed.
            eachMessage: async ({ topic, partition, message }) => {
                console.log("Topic Name::", topic);
                console.log("Partitions", partition);
                console.log("Message::\n", {
                    key: message.key.toString(),
                    value: message.value.toString(),
                    headers: message.headers,
                })
            }
        })
    }
}
