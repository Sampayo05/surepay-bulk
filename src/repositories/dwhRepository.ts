// src/repositories/defaultRepository.ts
import dbDwh from "../configs/db";

class DwhRepository {
    async updateMsisdnValDateStatus(update_row: number, update_comments: string, msisdn: string): Promise<any> {
        return await dbDwh.query(
            `UPDATE dtm_ext_srp_valdate
             SET update_row = $1,
                 update_comments = $2
             where msisdn = $3
               and report_date = NOW()::DATE;`,
            [update_row, update_comments, msisdn]
        );
    }
}

export default new DwhRepository();