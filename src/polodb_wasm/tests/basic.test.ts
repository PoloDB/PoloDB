import { describe, beforeAll, it, expect } from "vitest";
import init, { DatabaseDelegate } from "../dist";

describe("Basic", () => {

    beforeAll(async () => {
        await init();
    })

    it("should be 1", () => {
        expect(1).toBe(1)
    });

    it("should be", async () => {
        const db = await DatabaseDelegate.open("test");
        const collection = db.collection("test");
        // collection.insertOne({
        //     _id: "name",
        // })
        // exp
    })


});
