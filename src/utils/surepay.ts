// src/services/surepay.ts
import axios, { AxiosError } from "axios";
import {parseStringPromise} from "xml2js";
import {logger} from "../configs/logger";
import utilities from "./utilities";
import { sendSms } from "../service/smsService";

const utils = new utilities();

export async function subscribeToZeroRating(
    url: string,
    msisdn: string | number,
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
        .replaceAll("{{MSISDN}}", '228' + utils.extractPhoneNumberPart(msisdn.toString()))
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

const SUREPAY_URL = process.env.SUREPAY_URL || "http://10.2.1.1:8081/ecgs/gateway";

function buildSurepayXml(mobile: string, transId: string, amount: string | number, bid: string): string {
    return `<!DOCTYPE GatewayRequest SYSTEM "http://10.2.1.1:8081/ecgs/dtd/gateway.dtd">
<GatewayRequest>
   <RequestHeader version="{{SUREPAY_VERSION}}"/>
   <SubscriberAccountInfo>
      <SubscriberID>${mobile}</SubscriberID>
   </SubscriberAccountInfo>
   <QueryDataRequest OP="A" Action="IMOM" IMOMCommand="SCB:EBUCKET,MOBILE=${mobile},BUCKETSOURCEID=${bid},AMOUNT=${amount},TRANS_ID=${transId},IDTYPE=S"/>
</GatewayRequest>`;
}


async function surePayTopUpFirst(mobile: string): Promise<{ success: boolean; message: string }> {
    const msisdn = "228" + utils.extractPhoneNumberPart(mobile.toString());
    const transId = "SBF" + mobile + Date.now();

    const xmlData = buildSurepayXml(msisdn, transId, 0, "BID-FIRST");

    try {
        const { data } = await axios.post(SUREPAY_URL, xmlData, {
            headers: { "Content-Type": "application/xml" },
            timeout: 15000,
        });

        const parsed = await parseStringPromise(data, { explicitArray: true, attrkey: "$" });
        const responseHeader = parsed?.GatewayResponse?.ResponseHeader?.[0]?.$;

        if (responseHeader?.result_code === "00" && responseHeader?.additional_info === "SUCCESS") {
            return { success: true, message: "OK" };
        }
        return { success: false, message: responseHeader?.additional_info || "Erreur inconnue" };
    } catch (error) {
        const err = error as AxiosError;
        return { success: false, message: err.message };
    }
}


async function surePayTopUp(mobile: string, amount: string): Promise<{ success: boolean; message: string }> {
    const msisdn = "228" + utils.extractPhoneNumberPart(mobile.toString());
    const transId = "SB" + mobile + Date.now();

    const xmlData = buildSurepayXml(msisdn, transId, amount, "BID-TOPUP");

    try {
        const { data } = await axios.post(SUREPAY_URL, xmlData, {
            headers: { "Content-Type": "application/xml" },
            timeout: 15000,
        });

        const parsed = await parseStringPromise(data, { explicitArray: true, attrkey: "$" });
        const responseHeader = parsed?.GatewayResponse?.ResponseHeader?.[0]?.$;

        if (responseHeader?.result_code === "00" && responseHeader?.additional_info === "SUCCESS") {
            return { success: true, message: "CREDITED" };
        }
        return { success: false, message: responseHeader?.additional_info || "Erreur crédit" };
    } catch (error) {
        const err = error as AxiosError;
        return { success: false, message: err.message };
    }
}


export async function dailyTraitement(
    rembourseDataList: { mobile: string; data: string }[]
): Promise<{ status: number; mobile: string }[]> {
    logger.info("Début du traitement avec Surepay");

    const results: { status: number; mobile: string }[] = [];

    for (const rembourseData of rembourseDataList) {
        try {
         
            const check = await surePayTopUpFirst(rembourseData.mobile);
            if (!check.success) {
                logger.error(`Échec vérification initiale pour ${rembourseData.mobile} :: ${check.message}`);
                results.push({ status: -1, mobile: rembourseData.mobile });
                continue;
            }

            
            const response = await surePayTopUp(rembourseData.mobile, rembourseData.data);
            if (response.success) {
                logger.info(`Succès du crédit pour ${rembourseData.mobile} :: data=${rembourseData.data}`);

         
                const valeur = Number(rembourseData.data);
                const dataGo = Math.floor(valeur / Math.pow(1024, 2));
                const dataMo = Math.round((((valeur / Math.pow(1024, 2)) - dataGo) * 1024) * 100) / 100;

               
                await sendSms(
                    rembourseData.mobile,
                    `Togocom vous offre ${dataGo}Go ${String(dataMo).replace(".", ",")}Mo, valable entre 20H et 00H00. Profitez-en ! Solde : *909*0#`
                );

                results.push({ status: 1, mobile: rembourseData.mobile });
            } else {
                logger.error( `Échec du crédit pour ${rembourseData.mobile} :: ${response.message}`);
                results.push({ status: -1, mobile: rembourseData.mobile });
            }
        } catch (error: any) {
            logger.error( `Erreur pendant le traitement du numéro ${rembourseData.mobile} :: ${error.message}`);
            results.push({ status: -1, mobile: rembourseData.mobile });
        }
    }

    logger.info("Fin du traitement avec Surepay.");
    return results;
}
