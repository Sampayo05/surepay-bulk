// src/config/DWHDb.ts
import dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

const poolDbDWH = new Pool({
    host: process.env.DWH_DB_HOST as string,
    port: parseInt(process.env.DWH_DB_PORT as string, 10),
    user: process.env.DWH_DB_USERNAME as string,
    password: process.env.DWH_DB_PASSWORD as string,
    database: process.env.DWH_DB_NAME as string,
});

export default poolDbDWH;