/**
 * Manual end-to-end check against a real provider. Needs credentials and network,
 * so it is not part of `npm test`.
 *
 *   node src/smoke.ts [provider] [model-id]
 *
 * Reads the provider's baseUrl and model list from ~/.pi/agent/models.json and its
 * key from ~/.pi/agent/auth.json. The key is never printed.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Api, Model } from "@earendil-works/pi-ai";
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import { streamOverWebSocket } from "../index.ts";
import { createStats } from "./ws-transport.ts";

const providerName = process.argv[2] ?? "my-provider";
const wantedModel = process.argv[3];

const agentDir = getAgentDir();
const readJson = (name: string): Record<string, any> =>
	JSON.parse(readFileSync(join(agentDir, name), "utf-8")) as Record<string, any>;

const provider = readJson("models.json").providers?.[providerName];
if (!provider) throw new Error(`provider ${providerName} not found in models.json`);

const modelConfig = wantedModel
	? provider.models?.find((entry: { id: string }) => entry.id === wantedModel)
	: provider.models?.[0];
if (!modelConfig) throw new Error(`no model ${wantedModel ?? "(first)"} for provider ${providerName}`);

const credential = readJson("auth.json")[providerName];
const apiKey: string | undefined = credential?.key ?? credential?.apiKey ?? credential?.access;
if (!apiKey) throw new Error(`no usable credential for ${providerName} in auth.json`);

const model: Model<Api> = {
	...modelConfig,
	api: "openai-responses",
	provider: providerName,
	baseUrl: provider.baseUrl,
};

const stats = createStats();
const stream = streamOverWebSocket(
	model,
	{ messages: [{ role: "user", content: "Reply with exactly: pong", timestamp: Date.now() }] },
	{ apiKey, transport: "websocket", websocketConnectTimeoutMs: 15_000 },
	{
		stats,
		unsupportedScope: providerName,
		settings: { providers: [providerName], transport: "websocket", connectTimeoutMs: 15_000 },
		warnFallback: (reason) => console.error(`fallback: ${reason}`),
	},
);

console.error(`provider=${providerName} model=${model.id} base=${provider.baseUrl}`);

for await (const event of stream) {
	if (event.type === "text_delta") process.stderr.write(event.delta);
	else if (event.type === "thinking_delta") process.stderr.write(".");
	else if (event.type === "error") console.error(`\nERROR: ${event.error.errorMessage}`);
	else if (event.type === "done") {
		console.error(`\ndone reason=${event.reason} usage=${JSON.stringify(event.message.usage)}`);
	}
}

console.error(
	`stats attempts=${stats.attempts} connected=${stats.connected} full=${stats.fullRequests} ` +
		`delta=${stats.deltaRequests} sseFallbacks=${stats.sseFallbacks}` +
		(stats.lastError ? ` lastError="${stats.lastError}"` : ""),
);
