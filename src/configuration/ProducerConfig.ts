import { KafkaClient } from "./KafkaClient";
import { Producer, Transaction, ProducerRecord } from "kafkajs";

/**
 * Producer Config Class.
 */
export class ProducerConfig {
  // Kafka Client instance.
  private kafkaClient: KafkaClient;

  // Kafka Producer instance.
  private kafkaProducer: Producer;

  private MyPartitioner = () => {
    return ({}) => {
      // select a partition based on some logic
      // return the partition number
      return 0;
    };
  };
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
    console.log("publishMessageToTopicWithTransaction");
    if (induceError) {
      let invalidProducer = this.kafkaClient.kafkaProperties().producer({
        createPartitioner: this.MyPartitioner,
        maxInFlightRequests: 1,
        idempotent: true,
        transactionalId: "transactional-id",
      });
      let invalidKafkaTransaction: Transaction = await invalidProducer.transaction();

      try {
        invalidKafkaTransaction.send(producerRecord);
        throw "exception";
        await invalidKafkaTransaction.commit();
      } catch (error) {
        console.log(error);
        await invalidKafkaTransaction.abort();
        await this.publishMessageToErrorTopic(producerRecord);
      }
    } else {
      let kafkaTransaction: Transaction = await this.kafkaProducer.transaction();

      try {
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
      }
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
    producerRecord: ProducerRecord
  ): Promise<any> {
    let errorTopic: string = "errorTopic";
    producerRecord.topic = errorTopic;
    this.kafkaProducer.send(producerRecord);
  }
}
