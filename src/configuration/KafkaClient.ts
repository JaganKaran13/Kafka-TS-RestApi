import { Kafka } from "kafkajs";
import * as fs from "fs";
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
      clientId: "kafka-producer",
      // List of brokers available and can be used in the function
      brokers: ["localhost:9093"],
      ssl: {
        rejectUnauthorized: false,
        ca: [
          fs.readFileSync(
            "/home/jagan/Desktop/Security/ca-cert",
            "utf-8"
          ),
        ],
        cert: fs.readFileSync(
          "/home/jagan/Desktop/Security/local-client-cert-signed"
        ),
        key: fs.readFileSync("/home/jagan/Desktop/Security/client.pem"),
        passphrase: "123456",
      },
    });
    return kafka;
  }
}
