// src/routes/reverseRoute.ts
import type { Application } from "express";
import reverseController from "../controllers/reverseController";

export function registerReverseRoutes(app: Application) {
    app.get("/", reverseController.homepageController);
}
