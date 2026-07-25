/**
 * Manual multi-turn check: does `previous_response_id` continuation actually engage
 * against a real provider, and does the model still see the earlier turns?
 *
 *   node src/smoke-continuation.ts [provider] [model-id]
 *
 * A delta only goes out when the second request's input begins with exactly the
 * first request's input plus the items the server reported. That equality is the
 * whole risk, and it can only be confirmed against a live endpoint.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Api, AssistantMessage, Context, Message, Model } from "@earendil-works/pi-ai";
import { getAgentDir } from "@earendil-works/pi-coding-agent";
import { streamOverWebSocket } from "../index.ts";
import { SocketPool } from "./continuation.ts";
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

const model: Model<Api> = { ...modelConfig, api: "openai-responses", provider: providerName, baseUrl: provider.baseUrl };

const stats = createStats();
const pool = new SocketPool();
const deps = {
	stats,
	settings: { providers: [providerName], transport: "websocket-cached" as const, connectTimeoutMs: 15_000 },
	warnFallback: (reason: string) => console.error(`fallback: ${reason}`),
	pool,
};

const sessionId = `smoke-${Date.now()}`;

async function turn(messages: Message[], label: string): Promise<AssistantMessage> {
	const context: Context = { messages };
	const before = { delta: stats.deltaRequests, full: stats.fullRequests, reused: stats.connectionsReused };
	const stream = streamOverWebSocket(model, context, { apiKey, sessionId, transport: "websocket-cached" }, deps);

	let text = "";
	let final: AssistantMessage | undefined;
	for await (const event of stream) {
		if (event.type === "text_delta") text += event.delta;
		else if (event.type === "error") throw new Error(event.error.errorMessage ?? "stream error");
		else if (event.type === "done") final = event.message;
	}
	if (!final) throw new Error("no final message");

	console.error(
		`${label}: reply=${JSON.stringify(text.trim().slice(0, 60))} ` +
			`mode=${stats.deltaRequests > before.delta ? "DELTA" : "full"} ` +
			`reusedSocket=${stats.connectionsReused > before.reused} ` +
			`inputTokens=${final.usage.input}`,
	);
	return final;
}

const first: Message[] = [
	{ role: "user", content: "The secret word is ALPHA. Reply with just: ok", timestamp: Date.now() },
];
const a1 = await turn(first, "turn1");

const second: Message[] = [...first, a1, { role: "user", content: "Reply with just: ok", timestamp: Date.now() }];
const a2 = await turn(second, "turn2");

const third: Message[] = [
	...second,
	a2,
	{ role: "user", content: "What is the secret word? Reply with one word.", timestamp: Date.now() },
];
await turn(third, "turn3");

console.error(
	`\nstats attempts=${stats.attempts} connected=${stats.connected} reused=${stats.connectionsReused} ` +
		`full=${stats.fullRequests} delta=${stats.deltaRequests} sseFallbacks=${stats.sseFallbacks}` +
		(stats.strippedParams.length ? ` stripped=${stats.strippedParams.join(",")}` : ""),
);
pool.closeAll();
