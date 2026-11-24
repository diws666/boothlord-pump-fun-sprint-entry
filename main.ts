// Entry point for the pump.fun paper-trading replay (agent-fun/replay/v2).
// The grader runs: node /bot/main.ts  (Node strips the types; no build, no install).
//
// The strategy lives in strategy.ts (launch scalper: small early stakes on coins past
// 1 SOL of real reserve, flipped fast with a stop/trailing stop/time exit). Parameters
// were tuned on the dev-window simulation in scratch/ (see sweep results there).
// vendor/makefun-bot.ts is a byte-identical copy of the kit's sdk/ts/makefun-bot.ts.
//
// Determinism hard gate: every reply must be a pure function of the hello + batch
// stream. This file imports nothing else, reads no clock, no environment, no
// Math.random, no network and no files outside the repo, so two executions produce
// byte-identical action logs.

import { runBot } from './vendor/makefun-bot.ts';
import { createStrategy, DEFAULT_PARAMS } from './strategy.ts';

const bot = createStrategy(DEFAULT_PARAMS);

await runBot({
  onHello(state) {
    const sol = (x: bigint) => (Number(x) / 1e9).toFixed(6);
    console.error(`launch-scalper: hello capital ${sol(BigInt(state.hello.capital.lamports))} SOL ` +
      `caps ${JSON.stringify(state.hello.caps)} latency ${state.hello.latency_slots} slots`);
  },
  onBatch: (state, batch) => bot.onBatch(state, batch),
  onDone: (state, done) => bot.onDone?.(state, done),
});
