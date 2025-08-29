// src/routes/defaultRoute.ts
import type { Application } from "express";
import defaultController from "../controllers/defaultController";

export function registerDefaultRoutes(app: Application) {
    app.get("/", defaultController.homepageController);
}
