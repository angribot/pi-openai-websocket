/**
 * Manual end-to-end check against a real provider. Needs credentials and network,
 * so it is not part of `npm test`.
 *
 *   node src/smoke.ts [provider] [model-id]
 */

import { streamOverWebSocket } from "../index.ts";
import { SocketPool } from "./continuation.ts";
import { StickySseSessions } from "./session-fallback.ts";
import { loadTarget } from "./smoke-support.ts";
import { createStats, formatStats } from "./ws-transport.ts";

const providerName = process.argv[2] ?? "my-provider";
const { model, apiKey, baseUrl } = loadTarget(providerName, process.argv[3]);

const stats = createStats();
const stream = streamOverWebSocket(
	model,
	{ messages: [{ role: "user", content: "Reply with exactly: pong", timestamp: Date.now() }] },
	{ apiKey, transport: "websocket", websocketConnectTimeoutMs: 15_000 },
	{
		stats,
		settings: { providers: [providerName], transport: "websocket", connectTimeoutMs: 15_000 },
		warnFallback: (reason) => console.error(`fallback: ${reason}`),
		pool: new SocketPool(),
		stickySseSessions: new StickySseSessions(),
	},
);

console.error(`provider=${providerName} model=${model.id} base=${baseUrl}`);

for await (const event of stream) {
	if (event.type === "text_delta") process.stderr.write(event.delta);
	else if (event.type === "thinking_delta") process.stderr.write(".");
	else if (event.type === "error") console.error(`\nERROR: ${event.error.errorMessage}`);
	else if (event.type === "done") {
		console.error(`\ndone reason=${event.reason} usage=${JSON.stringify(event.message.usage)}`);
	}
}

console.error(`stats ${formatStats(stats)}`);
