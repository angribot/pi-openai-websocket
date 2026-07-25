/**
 * Shared setup for the manual smoke scripts. Reads a provider and model out of the pi
 * agent directory's `models.json`, and its credential out of `auth.json`.
 *
 * The credential is returned for use, never printed.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Api, Model } from "@earendil-works/pi-ai/compat";
import { getAgentDir } from "@earendil-works/pi-coding-agent";

export interface SmokeTarget {
	model: Model<Api>;
	apiKey: string;
	baseUrl: string;
}

export function loadTarget(providerName: string, modelId?: string): SmokeTarget {
	const agentDir = getAgentDir();
	const readJson = (name: string): Record<string, any> =>
		JSON.parse(readFileSync(join(agentDir, name), "utf-8")) as Record<string, any>;

	const provider = readJson("models.json").providers?.[providerName];
	if (!provider) throw new Error(`provider ${providerName} not found in models.json`);

	const modelConfig = modelId
		? provider.models?.find((entry: { id: string }) => entry.id === modelId)
		: provider.models?.[0];
	if (!modelConfig) throw new Error(`no model ${modelId ?? "(first)"} for provider ${providerName}`);

	const credential = readJson("auth.json")[providerName];
	const apiKey: string | undefined = credential?.key ?? credential?.apiKey ?? credential?.access;
	if (!apiKey) throw new Error(`no usable credential for ${providerName} in auth.json`);

	return {
		model: { ...modelConfig, api: "openai-responses", provider: providerName, baseUrl: provider.baseUrl },
		apiKey,
		baseUrl: provider.baseUrl,
	};
}
