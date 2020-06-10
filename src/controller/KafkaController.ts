import { JsonController, Body, Post, Req, Res } from 'routing-controllers';
import { UserModel } from '../models/UserModel';
import { Request, Response } from 'express';
import * as Ajv from 'ajv';
import { ProducerConfig } from '../configuration/ProducerConfig';

// Ajv instance
const AJV = new Ajv({ allErrors: true });

/**
 * Kafka Source Controller.
 */
@JsonController('/kafka')
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
    @Post('/')
    public async produceMessage(@Req() req: Request, @Res() res: Response, @Body() userDetails: UserModel): Promise<any> {
        try {
            let topicName: string = 'transaction';
            let key: string = "sampleKey";
            // validate the schema, if valid message is published to the topic.
            if (this.validateSchema(userDetails)) {
                await this.kafkaProducer.publishMessageToTopic(topicName, key, userDetails);
                return res.sendStatus(200);
            } else {
                // bad request is thrown to the user.
                console.log(AJV.errors);
                throw 'Invalid Schema';
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
        let valid: boolean | PromiseLike<any> = AJV.validate(UserModel.USER_SCHEMA, userDetails);
        // return true or false
        return valid;
    }
}
