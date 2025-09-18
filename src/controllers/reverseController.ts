// src/controllers/reverseController.ts
import type { Request, Response } from "express";

export default {
    homepageController: (req: Request, res: Response) => {
        res.json({ message: "Welcome to " + process.env.APP_NAME });
    },
};
