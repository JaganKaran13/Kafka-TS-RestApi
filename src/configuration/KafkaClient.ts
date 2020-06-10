import { Kafka } from 'kafkajs';

/**
 * Kafka Client Class.
 */export class KafkaClient {

    /**
     * Function which returns kafka configurations.
     */
    public kafkaProperties(): Kafka {
        // Kafka Configuration Settings.
        const kafka: Kafka = new Kafka({
            // Kafka Client id
            clientId: 'kafka-producer',
            // List of brokers available and can be used in the function
            brokers: ['localhost:9092'],
            // Timeout in ms untill a successful connection is available
            connectionTimeout: 3000,
            // Timeout in ms untill a successful request it available.
            requestTimeout: 25000,
            // Retry Configuraiton. Retries grows exponentially
            retry: {
                // Initial retry time in ms.
                initialRetryTime: 100,
                // Number of retries.
                retries: 3
            }
        })
        return kafka;
    }

}
