// Launch scalper for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Edge: coins that have absorbed about 1 SOL of real buying early in their life show short
// order-flow persistence. The strategy buys one small stake in young coins once the real
// reserve is large enough for a qualifying buy (>= 0.01 SOL within the 1%-of-reserve cap, so
// every buy counts toward the round-trip guardrail), with real net buying and at least one
// non-creator buyer, and only when the price hasn't already run away from its create price.
// Losers are cut at -40%, winners are run with a trailing stop (armed at +20%, 10% fall from
// peak), and anything still open after 1000 slots is flattened. One buy per coin, no
// averaging down.
//
// Every decision is a pure function of the batch stream (insertion-ordered maps only): no
// clock, no randomness, no network, no file reads - two executions log actions byte-for-byte
// alike. It imports nothing but the vendored wire shim.

import type { Batch, BotState, Done, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

const BPS = 10_000n;
const CURVE_FEE_BPS = 125n; // 95 bps protocol + 30 bps creator fallback

export interface Params {
  /** Lamports per coin (one buy only, within the 1%-of-real-reserve cap). */
  stakeLamports: number;
  /** Smallest buy worth a round trip (guardrail: >= 0.01 SOL buys count). */
  minBuyLamports: number;
  /** Earliest slot since create at which a first buy may be decided. */
  minAge: number;
  /** Latest slot since create at which a first buy may still be decided. */
  maxAge: number;
  /** Net SOL bought since create required before entry. */
  minNetBuyLamports: number;
  /** Distinct non-creator buyers required before entry. */
  minDistinctBuyers: number;
  /** Smallest real SOL reserve at which we enter (cap = 1% of it). */
  minRealSolLamports: number;
  /** Skip this batch when price is up more than this many bps from create price. */
  maxRunupBps: number;
  /** Sell when net value reaches cost * (1 + takeProfit). */
  takeProfitBps: number;
  /** Sell when net value falls to cost * (1 - stopLoss). */
  stopLossBps: number;
  /** Trailing stop: arms at cost * (1 + trailArm), exits on a `trail` fall from peak value. 0 = off. */
  trailArmBps: number;
  trailBps: number;
  /** Hard exit this many slots after the buy filled. */
  maxHoldSlots: number;
  /** Slippage tolerance on buys; sells always go out wide open. */
  buySlippageBps: number;
  /** Most new buys sent in one reply. */
  maxEntriesPerBatch: number;
}

// Shipped settings: 0.05 SOL stakes (capped by the reserve size cap) on coins past 1 SOL of
// real reserve, with early net buying and no big runup already in the price. Losers are cut
// at -40%, winners are run with a trailing stop (armed at +20%, 10% fall from peak), and
// anything still open after 1000 slots is flattened. Quick but not frenetic exits keep the
// fixed 105k lamports per order and ~12.5 bps curve fees per side from dominating stakes.
export const DEFAULT_PARAMS: Params = {
  stakeLamports: 50_000_000,      // 0.05 SOL
  minBuyLamports: 10_000_000,     // 0.01 SOL
  minAge: 1,
  maxAge: 40,
  minNetBuyLamports: 0,           // any net buying since create
  minDistinctBuyers: 1,
  minRealSolLamports: 1_000_000_000, // 1 SOL real reserve -> size cap >= 0.01 SOL
  maxRunupBps: 1000,              // skip batches where price is up > 10% from create price
  takeProfitBps: 10_000,          // take profit off: let the trailing stop run winners
  stopLossBps: 400,               // -40%
  trailArmBps: 200,               // arm trailing stop at +20% of cost
  trailBps: 100,                  // exit on a 10% fall from peak value
  maxHoldSlots: 1000,             // hard time limit (~7 minutes) or until complete/end
  buySlippageBps: 500,
  maxEntriesPerBatch: 8,
};

interface Tick {
  slot: number;
  buy: boolean;
  sol: bigint;
  user: Pubkey;
  price: number; // virtual quote / virtual token at the trade
}

interface Tape {
  mint: Pubkey;
  created: number | null;
  creator: Pubkey;
  createPrice: number; // vq / vt at the CreateEvent
  ticks: Tick[];
  netBuy: bigint; // SOL bought minus sold since create
  buyers: Set<Pubkey>;
  decided: boolean; // a buy was sent for this mint
}

interface Held {
  invested: bigint;   // SOL sent in the buy (fees included, from the fill)
  opened: number;     // slot the buy filled
  peakValue: bigint;  // best net close value seen so far
  buyLamports: bigint; // buy size, for round-trip bookkeeping
}

export function createStrategy(p: Params = DEFAULT_PARAMS) {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  const buyInFlight = new Set<Pubkey>();
  const sellInFlight = new Set<Pubkey>();
  let roundTrips = 0; // bought >= minBuy and fully exited (sell fill or harness close)

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = { mint, created: null, creator: '', createPrice: 0, ticks: [], netBuy: 0n, buyers: new Set(), decided: false };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Close value of `tokens` at reserves (vq, vt), after curve fees and one tx fee. */
  const netValue = (tokens: bigint, vq: bigint, vt: bigint, fee: bigint): bigint => {
    if (tokens <= 0n || vt <= 0n) return -fee;
    const gross = (tokens * vq) / (vt + tokens);
    const curveFee = (gross * CURVE_FEE_BPS + BPS - 1n) / BPS;
    return gross - curveFee - fee;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const fee = state.txFeeLamports();
      const orders: OrderInput[] = [];
      const orderCap = state.hello.caps.max_orders_per_batch;

      // ---- fills: reconcile our own orders (harness closes start with '_') ----
      for (const f of batch.fills) {
        if (f.client_id.startsWith('_')) continue;
        if (f.side === 'buy' && f.status === 'filled') {
          const lamports = BigInt(f.quote_lamports);
          let h = held.get(f.mint);
          if (h === undefined) {
            h = { invested: lamports, opened: f.slot, peakValue: 0n, buyLamports: lamports };
            held.set(f.mint, h);
          } else {
            h.invested += lamports;
            h.buyLamports += lamports;
          }
          buyInFlight.delete(f.mint);
        } else if (f.side === 'sell' && f.status === 'filled') {
          const h = held.get(f.mint);
          if (h !== undefined && h.buyLamports >= BigInt(p.minBuyLamports)) roundTrips += 1;
          held.delete(f.mint);
          sellInFlight.delete(f.mint);
        } else if (f.side === 'buy' && (f.status === 'rejected' || f.status === 'failed')) {
          buyInFlight.delete(f.mint);
        } else if (f.side === 'sell' && (f.status === 'rejected' || f.status === 'failed')) {
          sellInFlight.delete(f.mint);
        }
      }

      // ---- events: build the tapes -----------------------------------------
      for (const e of batch.events) {
        if (e.event === 'CreateEvent') {
          const t = tapeOf(e.mint);
          if (t.created === null) {
            t.created = e.slot;
            t.creator = e.creator;
            t.createPrice = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
          }
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = BigInt(e.sol_amount);
          const vq = Number(e.virtual_quote_reserves);
          const vt = Number(e.virtual_token_reserves);
          t.ticks.push({ slot: e.slot, buy: e.is_buy, sol, user: e.user, price: vt <= 0 ? 0 : vq / vt });
          t.netBuy += e.is_buy ? sol : -sol;
          if (e.is_buy && e.user !== t.creator) t.buyers.add(e.user);
        } else {
          // CompleteEvent: the harness closes positions in completed curves itself.
          const t = tapes.get(e.mint);
          if (t !== undefined) t.decided = true;
        }
      }

      // ---- exits first ------------------------------------------------------
      const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
      for (const m of batch.marks) marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });

      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        if (pos === undefined || pos.token_amount <= 0n) {
          held.delete(mint);
          sellInFlight.delete(mint);
          continue;
        }
        if (sellInFlight.has(mint)) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue; // harness closes completed curves itself
        const mk = marks.get(mint);
        const vq = mk === undefined ? curve.virtual_quote_reserves : mk.vq;
        const vt = mk === undefined ? curve.virtual_token_reserves : mk.vt;
        const net = netValue(pos.token_amount, vq, vt, fee);
        if (net > h.peakValue) h.peakValue = net;

        let exit = false;
        const invested = h.invested;
        if (invested > 0n) {
          if (net * BPS <= invested * (BPS - BigInt(p.stopLossBps))) exit = true; // stop loss
          if (!exit &&
              h.peakValue * BPS >= invested * (BPS + BigInt(p.trailArmBps)) &&
              net * BPS <= h.peakValue * (BPS - BigInt(p.trailBps))) exit = true; // trailing stop
        }
        if (!exit && now - h.opened > p.maxHoldSlots) exit = true; // time exit

        if (!exit) continue;
        if (orders.length >= orderCap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        sellInFlight.add(mint);
      }

      // ---- entries ----------------------------------------------------------
      let slotsLeft = state.hello.caps.max_open_positions - state.committedPositionCount();
      let free = state.freeCashLamports();
      let entries = 0;

      for (const t of tapes.values()) {
        if (entries >= p.maxEntriesPerBatch || orders.length >= orderCap || slotsLeft <= 0) break;
        const created = t.created;
        if (created === null) continue;
        const age = now - created;
        if (age < p.minAge || age > p.maxAge) continue;
        const mint = t.mint;
        if (t.decided || buyInFlight.has(mint) || held.has(mint) || sellInFlight.has(mint)) continue;

        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (curve.real_quote_reserves < BigInt(p.minRealSolLamports)) continue; // cap too small for a qualifying buy

        const last = t.ticks[t.ticks.length - 1];
        if (last === undefined) continue;
        const runupBps = t.createPrice <= 0 ? 0 : ((last.price - t.createPrice) * 10_000) / t.createPrice;
        if (runupBps > p.maxRunupBps) continue; // already ran away - skip this batch, try again later
        if (t.netBuy < BigInt(p.minNetBuyLamports)) continue;
        if (t.buyers.size < p.minDistinctBuyers) continue;

        const cap = state.maxBuyLamports(mint);
        let amount = BigInt(p.stakeLamports) < cap ? BigInt(p.stakeLamports) : cap;
        if (amount < BigInt(p.minBuyLamports)) continue;
        if (free < amount + fee) continue;

        orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: p.buySlippageBps });
        free -= amount + fee;
        slotsLeft -= 1;
        entries += 1;
        t.decided = true;
        buyInFlight.add(mint);
      }

      return orders;
    },
    onDone(state: BotState, done: Done): void {
      // Diagnostics only: stderr is not part of the action log.
      const sol = (x: bigint) => (Number(x) / 1e9).toFixed(6);
      console.error(`launch-scalper: done ${done.reason} roundTrips(>=minBuy) ${roundTrips} ` +
                    `cash ${sol(state.cash)} SOL`);
    },
    /** Diagnostic: round trips with a buy >= minBuyLamports that were fully exited. */
    roundTrips(): number {
      return roundTrips;
    },
  };
}
