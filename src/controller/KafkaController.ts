import { JsonController, Body, Post, Req, Res } from 'routing-controllers';
import { UserModel } from '../models/UserModel';
import { Request, Response } from 'express';
import * as Ajv from 'ajv';
import { kafkaProperties } from '../configuration/kafkaProperties';

// Ajv instance
const AJV = new Ajv({ allErrors: true });

/**
 * Kafka Source Controller.
 */
@JsonController('/kafka')
export class KafkaController {

    /**
     * Produce the message to the topic if valid.
     * 
     * @param req Request object. 
     * @param res Response object.
     * @param userDetails User Details received as body from the user.
     */
    @Post('/')
    async produceMessage(@Req() req: Request, @Res() res: Response, @Body() userDetails: UserModel): Promise<any> {
        try {
            // validate the schema, if valid message is published to the topic.
            if (validateSchema(userDetails)) {
                console.log(userDetails);
                produceMessageToTopic(userDetails);
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
}

/**
 * Validate the schema.
 * 
 * @param userDetails Details of the user from postman.
 */
function validateSchema(userDetails: UserModel): boolean | PromiseLike<any> {
    // Schema validation.
    let valid: boolean | PromiseLike<any> = AJV.validate(UserModel.USER_SCHEMA, userDetails);
    // return true or false
    return valid;
}

/**
 * Produce the userDetails to the topic.
 * 
 * @param userDetails  Details of the user from postman
 */
function produceMessageToTopic(userDetails: UserModel): void {
    // Kafka Configuration Settings.
    const kafka = kafkaProperties();

    // Kafka Producer instance.
    const producer = kafka.producer()

    // Producer instance connects with kafka with the configuration.
    producer.connect()

    // Send the message to topic
    producer.send({
        // Topic name
        topic: 'producerTopic',
        // Message to be produced in the topic.
        messages: [{
            key: 'INSERT',
            value: Buffer.from(JSON.stringify(userDetails))
        }]
    })

    // Disconnects the connection from producer.
    producer.disconnect();
}