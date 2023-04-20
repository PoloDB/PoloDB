import { describe, beforeAll, afterAll, it, expect } from "vitest";
import init, { DatabaseDelegate } from "../dist";

const TEST_DB_NAME = "test";

describe("Basic", () => {
    const dbs: DatabaseDelegate[] = [];

    beforeAll(async () => {
        await init();
        await DatabaseDelegate.drop(TEST_DB_NAME);
    });

    afterAll(() => {
        dbs.forEach(db => db.close());
    });

    it("should be", async () => {
        const db = await DatabaseDelegate.open(TEST_DB_NAME);
        dbs.push(db);
        const collection = db.collection("test");
        collection.insertOne({
            _id: "name",
            value: "content",
        })
        expect(collection.countDocuments()).toBe(1);
    })


});
