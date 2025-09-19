// src/utils/kafkaConnector.ts
import {Kafka, logLevel, EachMessagePayload} from "kafkajs";
import {logger} from "../configs/logger";
import {subscribeToZeroRating} from "./surepay";
import {subscribeToLifecycle} from "./surepay";
import {reverseData} from "./surepay";
import utilities from "./utilities";

interface DWHRecord {
    updateRow: number;
    updateComments: string;
    msisdns: string[];
}

interface RembourseRecord {
    status: number;
    statusComments: string;
    mobile: string[];
}


const BATCH_SIZE = 500;
const FLUSH_MS = 5000;

let batchToInsert: DWHRecord[] = [{
    updateRow: 1,
    updateComments: 'SUCCESS',
    msisdns: []
}];

let batchToInsertFailure: DWHRecord[] = [{
    updateRow: -1,
    updateComments: 'FAILURE',
    msisdns: []
}];

let batchReverseToInsert: RembourseRecord[] = [{ status: 1, statusComments: "SUCCESS", mobile: [] }];
let batchReverseToInsertFailure: RembourseRecord[] = [{ status: -1, statusComments: "FAILURE", mobile: [] }];


let flushTimer: NodeJS.Timeout | null = null;

async function flushBatchSuccess() {
    if (batchToInsert.length === 0) return;
    const snapshot = batchToInsert;
    batchToInsert = [{
        updateRow: 1,
        updateComments: 'SUCCESS',
        msisdns: []
    }];
    logger.info(`[BULK] to update ${snapshot.length} rows`);
    utils.insertIntoDWHDB(snapshot);
}

async function flushBatchFailure() {
    if (batchToInsertFailure.length === 0) return;
    const snapshot = batchToInsertFailure;
    batchToInsertFailure = [{
        updateRow: -1,
        updateComments: 'FAILURE',
        msisdns: []
    }];
    logger.info(`[FAILED BULK] to update ${snapshot.length} rows`);
    utils.insertIntoDWHDB(snapshot);
}

async function reverseFlushBatchFailure(){
    if(batchReverseToInsertFailure.length === 0) return;
    const snapshot = batchReverseToInsertFailure;
    batchReverseToInsertFailure =[{
        status: -1,
        statusComments: 'Failure',
        mobile: []
    }];
    logger.info(`[REVERSE FAILED BULK] to update ${snapshot.length} rows`);
    await utils.markRembourseAsProcessed(snapshot);
}

async function reverseFlushBatchSuccess() {
    if (batchReverseToInsert.length === 0) return;

    const snapshot = [...batchReverseToInsert];
    batchReverseToInsert = [{
        status: 1,
        statusComments: "SUCCESS",
        mobile: []
    }];

    logger.info(`[REVERSE BULK] to update ${snapshot.length} rows`);
    await utils.markRembourseAsProcessed(snapshot);
}


    
const utils = new utilities();

const {
    KAFKA_CLIENT_ID,
    KAFKA_HOST,
    KAFKA_PORT,
    KAFKA_TOPIC_DEFAULT,
    KAFKA_GROUP_ID_DEFAULT,
    SUREPAY_URL,
    SUREPAY_VERSION,
} = process.env;

if (!KAFKA_CLIENT_ID || !KAFKA_HOST || !KAFKA_PORT || !KAFKA_TOPIC_DEFAULT || !KAFKA_GROUP_ID_DEFAULT || !SUREPAY_URL || !SUREPAY_VERSION) {
    logger.error(
        "[Kafka] Missing env vars: KAFKA_CLIENT_ID, KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC, KAFKA_GROUP_ID, SUREPAY_URL"
    );
    // Optional: throw to fail fast
    // throw new Error("Missing required environment variables");
}

const kafka = new Kafka({
    clientId: KAFKA_CLIENT_ID || "diit",
    brokers: [`${KAFKA_HOST}:${KAFKA_PORT}`],
    logLevel: logLevel.NOTHING,
});

const consumer = kafka.consumer({groupId: KAFKA_GROUP_ID_DEFAULT!});

let started = false;

export async function defaultKafkaConnector(): Promise<void> {
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
        logger.info(`[Kafka] Connecting consumer (groupId=${KAFKA_GROUP_ID_DEFAULT})...`);
        await consumer.connect();
        logger.info(`[Kafka] Connected. Subscribing to topic=${KAFKA_TOPIC_DEFAULT}...`);

        if (!KAFKA_TOPIC_DEFAULT) {
            throw new Error("KAFKA_TOPIC is not set");
        }

        await consumer.subscribe({topic: KAFKA_TOPIC_DEFAULT, fromBeginning: true});
        logger.info(`[Kafka] Subscribed to ${KAFKA_TOPIC_DEFAULT}`);

        await consumer.run({
            eachMessage: async ({topic, partition, message}: EachMessagePayload) => {
                try {
                    const valueStr = message.value ? message.value.toString() : "";

                    logger.info('valueStr received ==> ' + valueStr)

                    let jsonMessage: any;

                    jsonMessage = JSON.parse(valueStr);

                    if (typeof jsonMessage === "string") {
                        jsonMessage = JSON.parse(jsonMessage); // double parse
                    }

                    logger.info('jsonMessage.operation ==> ' + jsonMessage.operation);

                    if (jsonMessage.operation === 'ztm') {
                        await subscribeToZeroRating(surepayUrl, jsonMessage.msisdn, 1048576, "ZTM", surepayVersion);
                    }

                    if (jsonMessage.operation === 'lifecycle') {
                        const res = await subscribeToLifecycle(surepayUrl, jsonMessage.msisdn, surepayVersion);

                        if (res.updateRow === -1) {
                            batchToInsertFailure[0]!.msisdns.push(res.msisdn);
                        } else {
                            batchToInsert[0]!.msisdns.push(res.msisdn);
                        }

                        logger.info("batchToInsertFailure[0].msisdns.length ==> " + batchToInsertFailure[0]!.msisdns.length);
                        logger.info("batchToInsert[0].msisdns.length ==> " + batchToInsert[0]!.msisdns.length);

                        if ((batchToInsert[0]!.msisdns.length + batchToInsertFailure[0]!.msisdns.length) >= BATCH_SIZE) {
                            await flushBatchSuccess();
                            await flushBatchFailure();
                        }
                    } 

                    if (jsonMessage.operation === "reversedata") {
                         const rembourseDataList = jsonMessage.dataList || [];
                         const results = await reverseData(rembourseDataList);

                        results.forEach(r => {
                           if (r.status === -1) {
                               batchReverseToInsertFailure[0]!.mobile.push(r.mobile);
                            } else {
                               batchReverseToInsert[0]!.mobile.push(r.mobile);
                            }
                        });

                        logger.info("batchReverseToInsertFailure[0].mobile.length ==> " + batchReverseToInsertFailure[0]!.mobile.length);
                        logger.info("batchReverseToInsert[0].mobile.length ==> " + batchReverseToInsert[0]!.mobile.length);

                        if ((batchReverseToInsert[0]!.mobile.length + batchReverseToInsertFailure[0]!.mobile.length) >= BATCH_SIZE) {
                        await reverseFlushBatchSuccess();
                        await reverseFlushBatchFailure();
                        }
                    }
                

                } catch (err: any) {
                    logger.error(
                        `[Kafka] Error processing message on ${topic}[p${partition}] offset=${message.offset} :: ${
                            err?.message || err
                        }`
                    );
                }
            },
        });

        logger.info(`[Kafka] Consumer running on ${KAFKA_TOPIC_DEFAULT}`);
    } catch (e: any) {
        logger.error(`[Kafka] Consumer start error :: ${e?.message || e}`);
        started = false;
    }
}

