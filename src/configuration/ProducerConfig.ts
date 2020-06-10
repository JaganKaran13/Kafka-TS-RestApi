import { KafkaClient } from "./KafkaClient";
import { Producer, Transaction } from "kafkajs";

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
    this.kafkaClient = new KafkaClient();
    this.kafkaProducer = this.kafkaClient
      .kafkaProperties()
      .producer({
        maxInFlightRequests: 1,
        idempotent: true,
        transactionalId: "transactional-id",
      });
  }

  /**
   * Publish the message to the topic.
   *
   * @param topicName Topic Name.
   * @param key Key value of the message.
   * @param message Message to be sent.
   */
  public async publishMessageToTopic(
    topicName: string,
    key: string,
    message: any
  ): Promise<void> {
    console.log("publishMessageToTopic");
    let kafkaTransaction: Transaction = await this.kafkaProducer.transaction();
    try {
      let iterator: number = 0;
      while (iterator < 3) {
        console.log("Iterator::" + iterator);
        await kafkaTransaction.send({
          // Topic name
          topic: topicName,
          // Message to be produced in the topic.
          messages: [
            {
              key: key,
              value: Buffer.from(JSON.stringify(message)),
            },
          ],
        });
        if (iterator == 2) {
          throw "Dummy error";
        }
        iterator = iterator + 1;
      }
      kafkaTransaction.sendOffsets({
        consumerGroupId: "restApplicationGroup",
        topics: [
          {
            topic: topicName,
            partitions: [
              {
                partition: 0,
                offset: "40",
              },
            ],
          },
        ],
      });
      await kafkaTransaction.commit();
    } catch (error) {
      console.log(error);
      await kafkaTransaction.abort();
    }
  }
}
