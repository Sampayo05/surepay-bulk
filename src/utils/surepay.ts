// src/services/surepay.ts
import axios from "axios";
import {parseStringPromise} from "xml2js";
import {logger} from "../configs/logger";
import utilities from "./utilities";

const utils = new utilities();

export async function subscribeToZeroRating(
    url: string,
    msisdn: string,
    amount: number | string,
    bid: string,
    surepayVersion: string
): Promise<void> {
    const requestBody = `<!DOCTYPE GatewayRequest SYSTEM "http://10.2.1.1:8081/ecgs/dtd/gateway.dtd">
<GatewayRequest>
   <RequestHeader version="{{SUREPAY_VERSION}}"/>
   <SubscriberAccountInfo>
      <SubscriberID>{{MSISDN}}</SubscriberID>
   </SubscriberAccountInfo>
   <QueryDataRequest OP="A" Action="IMOM" IMOMCommand="SCB:EBUCKET,MSISDN={{MSISDN}},BUCKETSOURCEID={{BID}},AMOUNT={{AMOUNT}},TRANS_ID={{TRANS_ID}},IDTYPE=S"/>
</GatewayRequest>`;

    const xmlData = requestBody
        .replaceAll("{{MSISDN}}", msisdn)
        .replaceAll("{{TRANS_ID}}", 'SB' + msisdn + Date.now().toString().slice(0, 6))
        .replaceAll("{{AMOUNT}}", String(amount))
        .replaceAll("{{BID}}", bid)
        .replaceAll("{{SUREPAY_VERSION}}", surepayVersion);

    logger.info('Surepay request ==> ' + xmlData);

    try {
        const response = await axios.post(url, xmlData, {
            headers: {"Content-Type": "application/xml"},
            timeout: 15_000,
        });

        if (response.status !== 200) {
            logger.error(
                `Surepay non-200 for ${msisdn} :: status=${response.status} :: body=${response.data}`
            );
            return;
        }

        const parsed = await parseStringPromise(String(response.data), {
            explicitArray: true,
            attrkey: "$",
        });

        const header = parsed?.GatewayResponse?.ResponseHeader?.[0];
        const resultCode: string | undefined = header?.$?.result_code;
        const additionalInfo: string | undefined = header?.$?.additional_info;

        if (resultCode === "00" && additionalInfo === "SUCCESS") {
            logger.info(
                `Successfully set bundle ${bid} for msisdn ${msisdn} (amount=${amount})`
            );
        } else if (resultCode === "98" && additionalInfo === "FAILURE") {
            logger.info(
                `Failed to set bundle ${bid} for msisdn ${msisdn} :: body=${response.data}`
            );
        } else {
            logger.error(
                `Unexpected surepay response for ${msisdn} :: resultCode=${resultCode} :: additionalInfo=${additionalInfo} :: body=${response.data}`
            );
        }
    } catch (error: any) {
        const status = error?.response?.status;
        const data = error?.response?.data;
        logger.error(
            `Error in subscribeToZeroRating for ${msisdn} :: ${error?.message || error} :: status=${status} :: body=${data}`
        );
    }
}

export async function subscribeToLifecycle(
    url: string,
    msisdn: string | number,
    surepayVersion: string
): Promise<{ updateRow: number; msisdn: string }> {
    const requestBody = `<!DOCTYPE GatewayRequest SYSTEM "http://10.2.1.1:8081/ecgs/dtd/gateway.dtd">
<GatewayRequest>
<RequestHeader version="{{SUREPAY_VERSION}}"/>
<SubscriberAccountInfo>
<SubscriberID>{{MSISDN}}</SubscriberID>
</SubscriberAccountInfo>
<QueryDataRequest Action="IMOM" OP="A" IMOMCommand="ADJ:BALANCE,MSISDN={{MSISDN}},AMOUNT=0,ADJ=INCR,BAL=P,NO_LC=N,RECHARGE=N,CED_Set=90"/>
</GatewayRequest>`;

    const xmlData = requestBody
        .replaceAll("{{MSISDN}}", '228' + utils.extractPhoneNumberPart(msisdn.toString()))
        .replaceAll("{{SUREPAY_VERSION}}", surepayVersion);

    logger.info('Surepay request subscribeToLifecycle ==> ' + xmlData);

    try {
        const response = await axios.post(url, xmlData, {
            headers: {"Content-Type": "application/xml"},
            timeout: 15_000,
        });

        if (response.status !== 200) {
            logger.error(
                `Surepay subscribeToLifecycle non-200 for ${msisdn} :: status=${response.status} :: body=${response.data}`
            );
            return {
                updateRow: -1,
                msisdn: msisdn.toString()
            }
        }

        const parsed = await parseStringPromise(String(response.data), {
            explicitArray: true,
            attrkey: "$",
        });

        const header = parsed?.GatewayResponse?.ResponseHeader?.[0];
        const resultCode: string | undefined = header?.$?.result_code;
        const additionalInfo: string | undefined = header?.$?.additional_info;


        if (resultCode === "00" && additionalInfo === "SUCCESS") {
            logger.info(
                `Successfully subscribeToLifecycle for msisdn ${msisdn}`
            );
            return {
                updateRow: 1,
                msisdn: msisdn.toString()
            }
        } else if (resultCode === "98" && additionalInfo === "FAILURE") {
            logger.info(
                `Failed subscribeToLifecycle for msisdn ${msisdn} :: body=${response.data}`
            );
            return {
                updateRow: -1,
                msisdn: msisdn.toString()
            }
        } else {
            logger.error(
                `Unexpected surepay response for ${msisdn} :: resultCode=${resultCode} :: additionalInfo=${additionalInfo} :: body=${response.data}`
            );
            return {
                updateRow: -1,
                msisdn: msisdn.toString()
            }
        }
    } catch (error: any) {
        const status = error?.response?.status;
        const data = error?.response?.data;
        logger.error(
            `Error in subscribeToLifecycle for ${msisdn} :: ${error?.message || error} :: status=${status} :: body=${data}`
        );
        return {
            updateRow: -1,
            msisdn: msisdn.toString()
        }
    }
}
