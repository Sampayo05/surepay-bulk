interface DWHRecord {
    updateRow: number;
    updateComments: string;
    msisdns: string[];
}

import {logger} from "../configs/logger";
import dwhRepository from "../repositories/dwhRepository";

class Utilities {

    extractPhoneNumberPart(number: string | number): string {
        const prefixes: string[] = ['228', '+228', '00228'];

        let extractedNumber = number.toString();
        for (const prefix of prefixes) {
            if (extractedNumber.startsWith(prefix)) {
                extractedNumber = extractedNumber.slice(prefix.length);
                break;
            }
        }

        return extractedNumber;
    }

    async insertIntoDWHDB(batch: DWHRecord[]): Promise<void> {
        for (const record of batch) {
            logger.info(`Inserting row=${record.updateRow}, comment=${record.updateComments}, msisdn=${record.msisdns}`);
            dwhRepository.updateMsisdnsValDateStatus(record.updateRow, record.updateComments, record.msisdns);
        }
    }
}

export default Utilities;
