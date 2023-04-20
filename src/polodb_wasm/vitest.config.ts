import { defineConfig } from 'vitest/config'
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig({
    test: {
        browser: {
            enabled: true,
            name: 'chrome', // browser name is required
            provider: 'webdriverio',
        },
    },
    plugins: [
        wasm(),
        topLevelAwait(),
    ],
});
