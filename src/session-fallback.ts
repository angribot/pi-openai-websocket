export type TransportSetting = "sse" | "websocket" | "websocket-cached" | "auto";

export function shouldUseSse(
	transport: TransportSetting,
	sessionId: string | undefined,
	sessions: StickySseSessions,
): boolean {
	return transport === "sse" || sessions.isSseOnly(sessionId);
}

export class StickySseSessions {
	private readonly sessionIds = new Set<string>();

	isSseOnly(sessionId: string | undefined): boolean {
		return sessionId !== undefined && this.sessionIds.has(sessionId);
	}

	markSseOnly(sessionId: string | undefined): void {
		if (sessionId !== undefined) this.sessionIds.add(sessionId);
	}

	clear(sessionId?: string): void {
		if (sessionId === undefined) this.sessionIds.clear();
		else this.sessionIds.delete(sessionId);
	}
}
