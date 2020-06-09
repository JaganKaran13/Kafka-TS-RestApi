import "reflect-metadata"; // this shim is required
import * as bodyParser from "body-parser";
import { createExpressServer } from "routing-controllers";
import { KafkaController } from "./controller/KafkaController";

// creates express app, registers all controller routes and returns you express app instance
const app = createExpressServer({
    controllers: [KafkaController] // we specify controllers we want to use
});

app.use(bodyParser.json());

// run express application on port 1306
app.listen(1306), () => {
    console.log("App is running at http://localhost:1306")
};