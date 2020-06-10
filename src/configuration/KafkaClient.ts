import { Kafka } from 'kafkajs';

/**
 * Kafka Client Class.
 */
export class KafkaClient {

    /**
     * Function which returns kafka configurations.
     */
    public kafkaProperties(): Kafka {
        // Kafka Configuration Settings.
        const kafka: Kafka = new Kafka({
            // Kafka Client id
            clientId: 'kafka-producer',
            // List of brokers available and can be used in the function
            brokers: ['localhost:9092']
        })
        return kafka;
    }

}
