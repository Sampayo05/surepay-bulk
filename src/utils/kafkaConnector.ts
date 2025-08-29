// src/utils/kafkaConnector.ts
import { Kafka, logLevel, EachMessagePayload } from "kafkajs";
import { logger } from "../configs/logger";
import { subscribeToZeroRating } from "./surepay";

const {
    KAFKA_CLIENT_ID,
    KAFKA_HOST,
    KAFKA_PORT,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    SUREPAY_URL,
    SUREPAY_VERSION,
} = process.env;

if (!KAFKA_CLIENT_ID || !KAFKA_HOST || !KAFKA_PORT || !KAFKA_TOPIC || !KAFKA_GROUP_ID || !SUREPAY_URL|| !SUREPAY_VERSION) {
    logger.error(
        "[Kafka] Missing env vars: KAFKA_CLIENT_ID, KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC, KAFKA_GROUP_ID, SUREPAY_URL"
    );
    // Optional: throw to fail fast
    // throw new Error("Missing required environment variables");
}

const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID || "app-client",
    brokers: [`${KAFKA_HOST}:${KAFKA_PORT}`],
    logLevel: logLevel.NOTHING,
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID! });

let started = false;

export async function ztmKafkaConnector(): Promise<void> {
    if (started) {
        logger.info("[Kafka] connector already started; skipping.");
        return;
    }
    started = true;

    // Narrow SUREPAY_URL to a string before use
    const surepayUrl = SUREPAY_URL;
    const surepayVersion = SUREPAY_VERSION;

    if (!surepayUrl) {
        logger.error("[Kafka] SUREPAY_URL is not set; aborting consumer start.");
        started = false;
        return;
    }

    if (!surepayVersion) {
        logger.error("[Kafka] SUREPAY_VERSION is not set; aborting consumer start.");
        started = false;
        return;
    }

    try {
        logger.info(`[Kafka] Connecting consumer (groupId=${KAFKA_GROUP_ID})...`);
        await consumer.connect();
        logger.info(`[Kafka] Connected. Subscribing to topic=${KAFKA_TOPIC}...`);

        if (!KAFKA_TOPIC) {
            throw new Error("KAFKA_TOPIC is not set");
        }

        await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });
        logger.info(`[Kafka] Subscribed to ${KAFKA_TOPIC}`);

        await consumer.run({
            eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
                try {
                    const valueStr = message.value ? message.value.toString() : "";

                    await subscribeToZeroRating(surepayUrl, valueStr, 1048576, "ZTM", surepayVersion);
                } catch (err: any) {
                    logger.error(
                        `[Kafka] Error processing message on ${topic}[p${partition}] offset=${message.offset} :: ${
                            err?.message || err
                        }`
                    );
                }
            },
        });

        logger.info(`[Kafka] Consumer running on ${KAFKA_TOPIC}`);
    } catch (e: any) {
        logger.error(`[Kafka] Consumer start error :: ${e?.message || e}`);
        started = false;
    }
}

export async function stopDrawWinningsConnector(): Promise<void> {
    try {
        logger.info("[Kafka] Disconnecting consumer...");
        await consumer.disconnect();
        started = false;
        logger.info("[Kafka] Consumer disconnected.");
    } catch (e: any) {
        logger.error(`[Kafka] Disconnect error :: ${e?.message || e}`);
    }
}
