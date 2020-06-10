import { KafkaClient } from "./KafkaClient";
import { Producer } from "kafkajs";

/**
 * Producer Config Class.
 */
export class ProducerConfig {

    // Kafka Client instance.
    private kafkaClient: KafkaClient;

    // Kafka Producer instance.
    private kafkaProducer: Producer;

    /**
     * Constructor class which connect the service to kafka on application startup.
     */
    constructor() {
        console.log("Inside Producer COnfig constructor.")
        this.kafkaClient = new KafkaClient;
        this.kafkaProducer = this.kafkaClient.kafkaProperties().producer();
    }

    /**
     * Publish the message to the topic.
     * 
     * @param topicName Topic Name.
     * @param key Key value of the message.
     * @param message Message to be sent.
     */
    public publishMessageToTopic(topicName: string, key: string, message: any) {
        // Send the message to topic
        this.kafkaProducer.send({
            // Topic name
            topic: topicName,
            // Message to be produced in the topic.
            messages: [{
                key: key,
                value: Buffer.from(JSON.stringify(message))
            }]
        })

    }
}