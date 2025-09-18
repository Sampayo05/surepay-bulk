// src/service/smsService.ts
import axios from "axios";
import { logger } from "../configs/logger";

export async function sendSms(mobile: string, message: string): Promise<void> {
    try {
        const url = process.env.SMS_GATEWAY_URL;

        if (!url) {
            throw new Error("SMS_GATEWAY_URL n'est pas défini dans les variables d'environnement");
        }

        const payload = {
            to: mobile,
            text: message,
        };

        const response = await axios.post(url, payload, {
            headers: { "Content-Type": "application/json" },
            timeout: 10_000, 
        });

        if (response.status === 200) {
            logger.info(`SMS envoyé à ${mobile} :: ${message}`);
        } else {
            logger.error(
                `Échec envoi SMS vers ${mobile} :: status=${response.status} :: body=${JSON.stringify(response.data)}`
            );
           
            throw new Error(`SMS gateway error : ${response.status}`);
        }
    } catch (error: any) {
        logger.error(
            `Erreur envoi SMS vers ${mobile} :: ${error.message || error}`
        );
        throw error;
    }
}
