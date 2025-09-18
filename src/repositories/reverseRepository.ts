// src/repositories/reverseRepository.ts
import dbDwh from "../configs/db";

class ReverseRepository {
  
  async findByStatusAndReportDate(status: string, reportDate: string): Promise<any[]> {
    const result = await dbDwh.query(
      `SELECT * 
       FROM RembourseData 
       WHERE status = $1 AND reportDate = $2`,
      [status, reportDate]
    );
    return result.rows;
  }

  
  async findData(status: string, repDate: string, deb: number, fin: number): Promise<any[]> {
    const result = await dbDwh.query(
      `SELECT * 
       FROM RembourseData 
       WHERE status = $1 
       AND reportDate = $2 
       AND reporteId BETWEEN $3 AND $4 
       ORDER BY reporteId`,
      [status, repDate, deb, fin]
    );
    return result.rows;
  }


  async findByMobileAndReportDate(mobile: string, reportDate: string): Promise<any | null> {
    const result = await dbDwh.query(
      `SELECT * 
       FROM RembourseData 
       WHERE mobile = $1 AND reportDate = $2 
       LIMIT 1`,
      [mobile, reportDate]
    );
    return result.rows[0] || null;
  }

  
  async findByMobileAndReportDateAndData(mobile: string, reportDate: string, data: string): Promise<any | null> {
    const result = await dbDwh.query(
      `SELECT * 
       FROM RembourseData 
       WHERE mobile = $1 AND reportDate = $2 AND data = $3 
       LIMIT 1`,
      [mobile, reportDate, data]
    );
    return result.rows[0] || null;
  }

  
  async markAsProcessed(status: number, mobile: string): Promise<void> {
    await dbDwh.query(
      `UPDATE RembourseData
       SET status = $1, statusComments = 'Processed by reverse-data-service'
       WHERE mobile = $2`,
      [status, mobile]
    );
  }
}

export default new ReverseRepository();
