import "reflect-metadata"; // this shim is required
import * as bodyParser from "body-parser";
import { createExpressServer } from "routing-controllers";
import { KafkaController } from "./controller/KafkaController";
import { ConsumerConfig } from "./configuration/ConsumerConfig";

// creates express app, registers all controller routes and returns you express app instance
const app = createExpressServer({
  controllers: [KafkaController], // we specify controllers we want to use
});

// Parse the input to JSON format.
app.use(bodyParser.json());

// Initialize the consumer on application startup.
new ConsumerConfig("restApplicationGroup", "transaction");

// run express application on port 1306
app.listen(1306);
