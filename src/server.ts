import "reflect-metadata"; // this shim is required
import { createExpressServer } from "routing-controllers";
import { KafkaController } from "./controller/KafkaController";

// creates express app, registers all controller routes and returns you express app instance
const app = createExpressServer({
    controllers: [KafkaController] // we specify controllers we want to use
});

// run express application on port 3000
app.listen(1306);