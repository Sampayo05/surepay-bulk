import type { Request, Response } from "express";
import { subscribeToZeroRating } from "../utils/surepay";
import { logger } from "../configs/logger";

export const surepayController = {
    subscribe: async (req: Request, res: Response) => {
        try {
            const { url, msisdn, amount, bid, surepayVersion } = req.body;

            if (!url || !msisdn || !amount || !bid || !surepayVersion) {
                return res.status(400).json({
                    message:
                        "Missing required fields: url, msisdn, amount, bid, surepayVersion",
                });
            }

            await subscribeToZeroRating(url, msisdn, amount, bid, surepayVersion);

            return res.status(200).json({
                message: `Surepay subscription triggered for msisdn ${msisdn}`,
            });
        } catch (error: any) {
            logger.error(
                `Error in surepayController.subscribe :: ${error?.message || error}`
            );
            return res.status(500).json({ message: "Internal Server Error" });
        }
    },
};
