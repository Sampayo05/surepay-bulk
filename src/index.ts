import express from 'express';
import {logger} from './configs/logger'
import { registerDefaultRoutes } from "./routes/defaultRoute";
import { ztmKafkaConnector } from "./utils/kafkaConnector";

const index = express();
const port = process.env.APP_PORT;
const app_name = process.env.APP_NAME;

index.use(express.json());
registerDefaultRoutes(index);

index.listen(port, () => {
    logger.info(`Start ${app_name} on port:${port} ${ Date.now().toString().slice(0, 9)}`);
});

ztmKafkaConnector().catch((error) => {
    logger.error("Kafka handler error: " + (error?.message || String(error)));
});

process.on("exit", (code: number) => {
    logger.error(`Process exit with code ${code}`);
});

process.on("uncaughtException", (err: Error) => {
    logger.error("An error occurred: " + err.message);
    logger.error("Stack trace: " + err.stack);
    process.exit(1);
});

process.on("unhandledRejection", (reason: unknown) => {
    logger.error("Unhandled Rejection: " + String(reason));
    process.exit(1);
});