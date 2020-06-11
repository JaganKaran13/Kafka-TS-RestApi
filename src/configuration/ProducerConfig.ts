import { KafkaClient } from "./KafkaClient";
import { Producer, Transaction, ProducerRecord } from "kafkajs";
import { ErrorModel } from "../models/ErrorModel";

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
    this.kafkaProducer = this.kafkaClient.kafkaProperties().producer({
      maxInFlightRequests: 1,
      idempotent: true,
      transactionalId: "transactional-id",
    });
  }

  /**
   * Publish the message to the topic with Transaction.
   * 
   * @param producerRecord Producer Metadata.

   */
  public async publishMessageToTopicWithTransaction(
    producerRecord: ProducerRecord,
    induceError: boolean
  ): Promise<void> {
    let kafkaTransaction: Transaction = await this.kafkaProducer.transaction();
    try {
      if (induceError) {
        await kafkaTransaction.send(producerRecord);
        throw new ErrorModel("400", "Bad Request");
      }
      await kafkaTransaction.send(producerRecord);
      await kafkaTransaction.sendOffsets({
        consumerGroupId: "restApplicationGroup",
        topics: [
          {
            topic: producerRecord.topic,
            partitions: [
              {
                partition: 0,
                offset: "30",
              },
            ],
          },
        ],
      });
      await kafkaTransaction.commit();
    } catch (error) {
      console.log(error);
      await kafkaTransaction.abort();
      await this.publishMessageToErrorTopic(producerRecord, error);
    }
  }

  /**
   * Publish the message to the topic without Transaction.
   *
   * @param producerRecord Producer Metadata.
   */
  public async publishMessageToTopicWithoutTransaction(
    producerRecord: ProducerRecord
  ): Promise<any> {
    this.kafkaProducer.send(producerRecord);
  }

  /**
   * Publish the message to the error topic.
   *
   * @param producerRecord Producer Metadata.
   */
  public async publishMessageToErrorTopic(
    producerRecord: ProducerRecord,
    error: ErrorModel
  ): Promise<any> {
    let errorTopic: string = "errorTopic";
    let newErrorRecord: ProducerRecord = {
      topic: errorTopic,
      messages: [
        {
          key: producerRecord.messages[0].key,
          value: producerRecord.messages[0].value,
          headers: {
            errorMessage: error.description,
          },
        },
      ],
    };
    this.kafkaProducer.send(newErrorRecord);
  }
}
