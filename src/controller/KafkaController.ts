import { JsonController, Body, Post, Req, Res } from "routing-controllers";
import { UserModel } from "../models/UserModel";
import { Request, Response } from "express";
import * as Ajv from "ajv";
import { ProducerConfig } from "../configuration/ProducerConfig";
import { ProducerRecord } from "kafkajs";

// Ajv instance
const AJV = new Ajv({ allErrors: true });

/**
 * Kafka Source Controller.
 */
@JsonController("/kafka")
export class KafkaController {
  // Kafka Producer instance.
  kafkaProducer: ProducerConfig;

  /**
   * Constructor to connect producer and consumer instance to the kafka server.
   */
  constructor() {
    this.kafkaProducer = new ProducerConfig();
  }
  /**
   * Produce the message to the topic if valid.
   *
   * @param req Request object.
   * @param res Response object.
   * @param userDetails User Details received as body from the user.
   */
  @Post("/")
  public async produceMessage(
    @Req() req: Request,
    @Res() res: Response,
    @Body() userDetails: UserModel
  ): Promise<any> {
    try {
      let topicName: string = "transaction";
      let key: string = "sampleKey";
      // validate the schema, if valid message is published to the topic.
      if (this.validateSchema(userDetails)) {
        let producerRecord: ProducerRecord = this.createProducerRecord(
          topicName,
          key,
          userDetails
        );
        await this.kafkaProducer.publishMessageToTopicWithTransaction(
          producerRecord,
          true
        );
        return res.sendStatus(200);
      } else {
        // bad request is thrown to the user.
        console.log(AJV.errors);
        throw "Invalid Schema";
      }
    } catch (exception) {
      console.log(exception);
      return res.sendStatus(400);
    }
  }

  /**
   * Validate the schema.
   * @param userDetails Details of the user from postman.
   */
  public validateSchema(userDetails: UserModel): boolean | PromiseLike<any> {
    // Schema validation.
    let valid: boolean | PromiseLike<any> = AJV.validate(
      UserModel.USER_SCHEMA,
      userDetails
    );
    // return true or false
    return valid;
  }

  /**
   * Create a producer record and publish the message.
   *
   * @param topicName Topic Name
   * @param key Key of the message
   * @param userDetails User Details
   */
  public createProducerRecord(
    topicName: string,
    key: string,
    userDetails: UserModel
  ): ProducerRecord {
    let producerRecord: ProducerRecord = {
      // Topic name
      topic: topicName,
      // Message to be produced in the topic.
      messages: [
        {
          key: key,
          value: Buffer.from(JSON.stringify(userDetails)),
        },
      ],
    };
    return producerRecord;
  }
}
