#!/usr/bin/env python3
"""
event_backlog.py — Event-level EL backlog estimator using eth_getLogs.

- Streams DepositEvent logs from the deposit contract (block-window pagination).
- Parses the deposit amount (gwei, little-endian) from event data.
- Hydrates block timestamp and tx.from (cached).
- Computes backlog strictly after the (processed_count-1)-th event.
- Supports a tiny append-only JSONL cache to avoid rescanning history.

Public API:
    get_el_backlog_estimate_events(
        rpc_url: str,
        deposit_contract: str,
        processed_count: int,
        start_block_hint: int = 0,
        end_block: int = -1,  # -1 = latest
        block_stride: int = 100_000,
        incomplete_weight_after_cutoff: float = 0.5,
        cache_path: str | None = None,
    ) -> tuple[int, int]

Returns:
    (pending_events, pending_gwei)
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from config import DEPOSIT_EVENT_TOPIC0


# === LOGGING (set to False to disable all logging) ===
ENABLE_LOGGING = True

def log(msg: str) -> None:
    """Simple logging that can be toggled off by setting ENABLE_LOGGING = False"""
    if ENABLE_LOGGING:
        print(f"[event_backlog] {msg}", flush=True)

WEI_PER_ETH = 10 ** 18
WEI_PER_GWEI = 10 ** 9
ETH32_WEI = 32 * WEI_PER_ETH

# ----------------------
# Data models
# ----------------------

@dataclass(frozen=True)
class DepEvt:
    block_number: int
    tx_index: int
    log_index: int
    tx_hash: str
    ts: int                 # unix seconds (block timestamp)
    frm: str                # transaction "from" (lowercase)
    amount_gwei: int        # deposit amount in gwei (little-endian in event)

    @property
    def amount_wei(self) -> int:
        return self.amount_gwei * WEI_PER_GWEI


# ----------------------
# JSON-RPC helpers
# ----------------------
class RpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: dict | None = None):
        super().__init__(f"RPC error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data or {}

class HttpError(RuntimeError):
    """Raised when an HTTP error occurs (e.g., 401 Unauthorized, 429 Rate Limit)"""
    def __init__(self, status_code: int, message: str, url: str):
        super().__init__(f"HTTP {status_code} error for {url}: {message}")
        self.status_code = status_code
        self.message = message
        self.url = url

def _rpc_call(rpc_url: str, method: str, params: list, timeout: int = 60, fallback_url: str | None = None) -> dict:
    """
    Makes an RPC call to rpc_url. If the result is null and fallback_url is provided,
    retries the call with the fallback_url.
    """
    try:
        resp = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Convert HTTP errors to our custom HttpError so cache handling can detect them
        raise HttpError(e.response.status_code, str(e), rpc_url) from e

    j = resp.json()
    if "error" in j and j["error"]:
        err = j["error"]
        raise RpcError(int(err.get("code", -1)), str(err.get("message", "")), err.get("data"))

    result = j["result"]

    # If result is null and we have a fallback URL, try the fallback
    if result is None and fallback_url and fallback_url != rpc_url:
        log(f"Primary RPC returned null for {method}, trying fallback URL")
        try:
            resp_fallback = requests.post(
                fallback_url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=timeout,
            )
            resp_fallback.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise HttpError(e.response.status_code, str(e), fallback_url) from e

        j_fallback = resp_fallback.json()
        if "error" in j_fallback and j_fallback["error"]:
            err = j_fallback["error"]
            raise RpcError(int(err.get("code", -1)), str(err.get("message", "")), err.get("data"))
        result = j_fallback["result"]
        if result is not None:
            log(f"Fallback RPC succeeded for {method}")

    return result


def _hex(i: int) -> str:
    return hex(i)

def _int(h: str) -> int:
    return int(h, 16)


# ----------------------
# DepositEvent parsing
# ----------------------

def _parse_deposit_amount_gwei_from_log_data(data_hex: str) -> int:
    """
    DepositEvent(bytes, bytes, bytes, bytes, bytes)
      pubkey(48), withdrawal_credentials(32), amount(8), signature(96), index(8)
    Event 'data' encodes 5 dynamic bytes args:
      [off0, off1, off2, off3, off4] + segments
    For amount (3rd), read pointer -> length (8) -> 8 bytes (little-endian gwei).
    """
    if data_hex.startswith("0x"):
        data_hex = data_hex[2:]
    data = bytes.fromhex(data_hex)

    if len(data) < 32 * 5:
        raise ValueError("DepositEvent data too short")

    off_amount = int.from_bytes(data[32*2:32*3], "big")
    if off_amount + 32 > len(data):
        raise ValueError("Amount offset out of bounds")

    amt_len = int.from_bytes(data[off_amount:off_amount+32], "big")
    if amt_len != 8:
        raise ValueError(f"Unexpected deposit amount length: {amt_len}")

    start = off_amount + 32
    end = start + 8
    if end > len(data):
        raise ValueError("Amount data out of bounds")

    amount_le = data[start:end]
    return int.from_bytes(amount_le, "little")


# ----------------------
# Block-window log streaming (avoids provider caps)
# ----------------------


from collections import deque

def _estimate_safe_stride(
    rpc_url: str,
    deposit_contract: str,
    start_block: int,
    max_logs: int = 8000,  # Stay under 10k limit with safety margin
    sample_size: int = 1000,
) -> int:
    """
    Estimate a safe block stride by sampling log density.
    Returns a conservative stride that should stay under max_logs.
    """
    log(f"Estimating safe stride by sampling {sample_size} blocks starting at {start_block}")
    address = deposit_contract.lower()
    if not address.startswith("0x"):
        address = "0x" + address

    try:
        # Sample a small window to estimate density
        params = [{
            "fromBlock": _hex(start_block),
            "toBlock": _hex(start_block + sample_size),
            "address": address,
        }]
        logs = _rpc_call(rpc_url, "eth_getLogs", params)
        log_count = len(logs)

        if log_count == 0:
            log("No logs in sample, using large stride: 100,000 blocks")
            return 100_000  # No logs, use large stride

        # Estimate: if we got log_count logs in sample_size blocks,
        # how many blocks for max_logs?
        density = log_count / sample_size
        safe_stride = int(max_logs / density) if density > 0 else 100_000
        clamped = max(1000, min(safe_stride, 100_000))
        log(f"Found {log_count} logs in {sample_size} blocks (density: {density:.4f} logs/block)")
        log(f"Calculated safe stride: {clamped:,} blocks")
        return clamped
    except Exception as e:
        log(f"Failed to estimate stride ({e}), using conservative default: 10,000 blocks")
        return 10_000  # Fallback to conservative default


def _iter_deposit_logs(
    rpc_url: str,
    deposit_contract: str,
    start_block: int,
    end_block: int,
    block_stride: int = 100_000,   # initial coarse window; we'll split below
    sleep_sec: float = 0.2,
    topics: list[str] | None = None,  # you may pass DepositEvent topic0 here to reduce volume
    fallback_url: str | None = None,
) -> Iterable[dict]:
    """
    Yield deposit-contract logs in ascending order while respecting providers' 10k-cap.
    Strategy:
      • Start with coarse windows.
      • For each window, try eth_getLogs.
      • On -32005, split the window. If provider suggests a subrange, use it **only if it shrinks**.
      • Keep splitting until single blocks; if a single block still overflows, fall back to receipts.
    """
    # normalize
    address = deposit_contract.lower()
    if not address.startswith("0x"):
        address = "0x" + address

    latest = _int(_rpc_call(rpc_url, "eth_blockNumber", [], fallback_url=fallback_url))
    lo = max(0, start_block)
    hi = latest if end_block < 0 else min(end_block, latest)

    log(f"Fetching logs from block {lo:,} to {hi:,} ({hi-lo+1:,} blocks)")

    # Auto-tune stride if using default and range is large
    if block_stride == 100_000 and (hi - lo) > 1_000_000:
        log(f"Large range detected, auto-tuning stride...")
        block_stride = _estimate_safe_stride(rpc_url, address, lo)

    log(f"Using block stride: {block_stride:,}")

    # Work queue of ranges to fetch (ascending overall, but we'll sort at emit time)
    work = deque()
    cur = lo
    while cur <= hi:
        rhi = min(cur + block_stride - 1, hi)
        work.append((cur, rhi))
        cur = rhi + 1

    log(f"Created {len(work)} work windows to process")

    # We'll buffer logs per window and emit in-order.
    def try_get_logs(a: int, b: int) -> list[dict]:
        params = [{
            "fromBlock": _hex(a),
            "toBlock": _hex(b),
            "address": address,
        }]
        if topics:
            params[0]["topics"] = topics
        return _rpc_call(rpc_url, "eth_getLogs", params, fallback_url=fallback_url)

    # Process windows in ascending order
    total_logs = 0
    windows_processed = 0
    while work:
        a, b = work.popleft()
        # Sanity
        if b < a:
            continue
        try:
            log(f"Fetching logs [{a:,} - {b:,}] ({b-a+1:,} blocks, {len(work)} remaining)")
            logs = try_get_logs(a, b)
            total_logs += len(logs)
            windows_processed += 1
            log(f"  ✓ Got {len(logs)} logs (total: {total_logs})")
            # Great—emit these (already in ascending block order)
            for lg in logs:
                yield lg
            time.sleep(sleep_sec)
            continue
        except RpcError as e:
            too_many = (e.code == -32005) or ("10000" in (e.message or "")) or ("more than" in (e.message or "").lower())
            if not too_many:
                # Some other RPC error; bubble up
                log(f"  ✗ RPC error {e.code}: {e.message}")
                raise

            log(f"  ⚠ Too many results for [{a:,} - {b:,}], splitting...")

            # If single block, fall back to receipts
            if a == b:
                log(f"  → Single block {a}, fetching via receipts")
                logs = _fetch_block_via_receipts(rpc_url, a, address, topics, fallback_url)
                total_logs += len(logs)
                log(f"  ✓ Got {len(logs)} logs via receipts (total: {total_logs})")
                for lg in logs:
                    yield lg
                time.sleep(sleep_sec)
                continue

            # Try provider's suggested subrange if it **strictly** shrinks [a,b]
            sug_lo = e.data.get("from")
            sug_hi = e.data.get("to")
            used_suggestion = False
            if isinstance(sug_lo, str) and isinstance(sug_hi, str) and sug_lo.startswith("0x") and sug_hi.startswith("0x"):
                s_lo = max(a, int(sug_lo, 16))
                s_hi = min(b, int(sug_hi, 16))
                # Only use if it strictly narrows the interval
                if a <= s_lo <= s_hi <= b and (s_lo > a or s_hi < b):
                    log(f"  → Using provider suggestion: [{s_lo:,} - {s_hi:,}]")
                    # process [a, s_lo-1], [s_lo, s_hi], [s_hi+1, b] in order
                    if a <= s_lo - 1:
                        work.appendleft((s_lo, s_hi))      # middle first
                        work.appendleft((a, s_lo - 1))     # left before middle
                        if s_hi + 1 <= b:
                            work.append((s_hi + 1, b))     # right after middle (enqueue at end)
                    else:
                        # no left part
                        work.appendleft((s_lo, s_hi))
                        if s_hi + 1 <= b:
                            work.appendleft((s_hi + 1, b))
                    used_suggestion = True

            if used_suggestion:
                # loop continues; we did not re-queue the original [a,b]
                continue

            # Fallback: plain bisection that **guarantees shrinkage**
            mid = (a + b) // 2
            log(f"  → Bisecting at {mid:,}: [{a:,} - {mid:,}] and [{mid+1:,} - {b:,}]")
            # process left then right
            work.appendleft((mid + 1, b))
            work.appendleft((a, mid))
            continue

    log(f"Completed: {windows_processed} windows, {total_logs} total logs fetched")


def _fetch_block_via_receipts(
    rpc_url: str,
    block_number: int,
    address: str,
    topics: list[str] | None,
    fallback_url: str | None = None,
) -> list[dict]:
    """
    Last-resort path when a single block has >10k matching logs and eth_getLogs refuses.
    We:
      - get the block with full transactions,
      - get each tx's receipt,
      - collect logs that match (address + topics[0] if provided),
      - synthesize eth_getLogs-like dicts (we already have the real structure from receipts).
    """
    block = _rpc_call(rpc_url, "eth_getBlockByNumber", [_hex(block_number), True], fallback_url=fallback_url)  # full tx objects
    txs = block.get("transactions", []) or []

    out: list[dict] = []
    topic0 = topics[0] if topics else None

    for tx in txs:
        txh = tx["hash"]
        r = _rpc_call(rpc_url, "eth_getTransactionReceipt", [txh], fallback_url=fallback_url)
        logs = r.get("logs", []) or []
        for lg in logs:
            if (lg.get("address", "").lower() != address):
                continue
            if topic0:
                # Only keep logs whose first topic matches DepositEvent
                t0 = (lg.get("topics") or [None])[0]
                if (t0 or "").lower() != topic0.lower():
                    continue
            out.append(lg)

    # Important: receipts preserve on-chain ordering within the block already
    out.sort(key=lambda lg: _int(lg.get("logIndex", "0x0")))
    return out


# ----------------------
# Hydration helpers (timestamps, from) with simple caches
# ----------------------
def _batch_hydrate_meta(
    rpc_url: str,
    raw_logs: List[dict],
    fallback_url: str | None = None,
    meta_cache_path: str | None = None
) -> List[DepEvt]:
    """
    Convert raw logs to DepEvt by attaching:
      - block timestamp (cache per block)
      - tx.from (cache per tx)
      - parsed amount_gwei
    Maintains ascending order by (block, txIndex, logIndex).

    meta_cache_path: Optional path to persist block/tx metadata cache between runs.
                     Now saved on *any* hydration error, not just rate limits.

    Smart fallback: If primary RPC fails for archive data, switches to fallback and retries
    primary every 1000 calls as data becomes more recent.
    """
    if not raw_logs:
        return []

    raw_logs.sort(key=lambda lg: (_int(lg["blockNumber"]),
                                  _int(lg.get("transactionIndex", "0x0")),
                                  _int(lg.get("logIndex", "0x0"))))

    # Load persistent cache if available
    block_ts_cache: Dict[int, int] = {}
    tx_from_cache: Dict[str, str] = {}

    if meta_cache_path and os.path.exists(meta_cache_path):
        try:
            with open(meta_cache_path, "r") as f:
                cache_data = json.load(f)
                block_ts_cache = {int(k): int(v) for k, v in cache_data.get("blocks", {}).items()}
                tx_from_cache = cache_data.get("txs", {})
            log(f"Loaded metadata cache: {len(block_ts_cache)} blocks, {len(tx_from_cache)} txs")
        except Exception as e:
            log(f"Warning: Failed to load metadata cache: {e}")

    evts: List[DepEvt] = []
    cache_dirty = False
    total_logs = len(raw_logs)
    processed_logs = 0

    # Smart fallback tracking for archive data
    use_fallback_for_tx = False
    tx_calls_since_retry = 0

    log(f"Processing {total_logs} logs for metadata hydration...")

    for lg in raw_logs:
        bn = _int(lg["blockNumber"])
        txh = lg["transactionHash"]
        txi = _int(lg.get("transactionIndex", "0x0"))
        lgi = _int(lg.get("logIndex", "0x0"))

        # timestamp
        ts = block_ts_cache.get(bn)
        if ts is None:
            log(f"  Fetching block {bn} timestamp [{processed_logs+1}/{total_logs}]")
            try:
                block = _rpc_call(rpc_url, "eth_getBlockByNumber", [_hex(bn), False], fallback_url=fallback_url)
                ts = _int(block["timestamp"])
                block_ts_cache[bn] = ts
                cache_dirty = True
            except Exception as e:
                # NEW: persist cache on *any* hydration error
                log(f"Error while fetching block {bn} timestamp ({e}). Persisting meta cache and re-raising.")
                if meta_cache_path and cache_dirty:
                    _save_meta_cache(meta_cache_path, block_ts_cache, tx_from_cache)
                raise

        # from
        frm = tx_from_cache.get(txh)
        if frm is None:
            log(f"  Fetching tx {txh[:10]}... sender [{processed_logs+1}/{total_logs}]")

            should_retry_primary = (tx_calls_since_retry >= 1000)
            effective_rpc = rpc_url
            effective_fallback = fallback_url

            if use_fallback_for_tx and fallback_url:
                if should_retry_primary:
                    log(f"  Retrying primary RPC after {tx_calls_since_retry} calls (block {bn})")
                    effective_rpc = rpc_url
                    effective_fallback = fallback_url
                    tx_calls_since_retry = 0
                else:
                    effective_rpc = fallback_url
                    effective_fallback = None

            try:
                tx = _rpc_call(effective_rpc, "eth_getTransactionByHash", [txh], fallback_url=effective_fallback)
                frm = tx["from"].lower()
                tx_from_cache[txh] = frm
                cache_dirty = True
                tx_calls_since_retry += 1

                if use_fallback_for_tx and should_retry_primary and effective_rpc == rpc_url:
                    log(f"  Primary RPC succeeded for recent data (block {bn}), switching back to primary")
                    use_fallback_for_tx = False
                    tx_calls_since_retry = 0

            except (RpcError, HttpError) as e:
                # Detect archive gap and try fallback once
                is_archive_issue = False
                if isinstance(e, RpcError):
                    msg = e.message.lower()
                    is_archive_issue = ("null" in msg or "not found" in msg or "missing trie node" in msg)

                if is_archive_issue and not use_fallback_for_tx and fallback_url:
                    log(f"  Primary RPC lacks archive data for tx {txh} (block {bn}), switching to fallback")
                    use_fallback_for_tx = True
                    tx_calls_since_retry = 0
                    try:
                        tx = _rpc_call(fallback_url, "eth_getTransactionByHash", [txh], fallback_url=None)
                        frm = tx["from"].lower()
                        tx_from_cache[txh] = frm
                        cache_dirty = True
                        tx_calls_since_retry += 1
                    except Exception as fallback_error:
                        # NEW: persist cache on *any* hydration error
                        log(f"Error on fallback while fetching tx {txh} ({fallback_error}). Persisting meta cache and re-raising.")
                        if meta_cache_path and cache_dirty:
                            _save_meta_cache(meta_cache_path, block_ts_cache, tx_from_cache)
                        raise
                else:
                    # NEW: persist cache on *any* hydration error
                    log(f"Error while fetching tx {txh} ({e}). Persisting meta cache and re-raising.")
                    if meta_cache_path and cache_dirty:
                        _save_meta_cache(meta_cache_path, block_ts_cache, tx_from_cache)
                    raise

        amt_gwei = _parse_deposit_amount_gwei_from_log_data(lg["data"])

        evts.append(DepEvt(
            block_number=bn, tx_index=txi, log_index=lgi,
            tx_hash=txh, ts=ts, frm=frm, amount_gwei=amt_gwei
        ))

        processed_logs += 1
        if processed_logs % 100 == 0 or processed_logs == total_logs:
            log(f"  Progress: {processed_logs}/{total_logs} logs processed ({100*processed_logs//total_logs}%)")

    # Save cache after successful processing
    if meta_cache_path and cache_dirty:
        _save_meta_cache(meta_cache_path, block_ts_cache, tx_from_cache)

    return evts



def _save_meta_cache(meta_cache_path: str, block_ts_cache: Dict[int, int], tx_from_cache: Dict[str, str]) -> None:
    """Helper to save metadata cache to disk."""
    try:
        os.makedirs(os.path.dirname(meta_cache_path) or ".", exist_ok=True)
        cache_data = {
            "blocks": {str(k): v for k, v in block_ts_cache.items()},
            "txs": tx_from_cache
        }
        with open(meta_cache_path, "w") as f:
            json.dump(cache_data, f)
        log(f"Saved metadata cache: {len(block_ts_cache)} blocks, {len(tx_from_cache)} txs")
    except Exception as e:
        log(f"Warning: Failed to save metadata cache: {e}")


# ----------------------
# Tiny append-only cache (JSONL + .meta.json)
# ----------------------

def _ev_to_dict(e: DepEvt) -> dict:
    return {"bn": e.block_number, "ti": e.tx_index, "li": e.log_index,
            "th": e.tx_hash, "ts": e.ts, "fr": e.frm, "ag": e.amount_gwei}

def _ev_from_dict(d: dict) -> DepEvt:
    return DepEvt(block_number=int(d["bn"]), tx_index=int(d["ti"]), log_index=int(d["li"]),
                  tx_hash=str(d["th"]), ts=int(d["ts"]), frm=str(d["fr"]), amount_gwei=int(d["ag"]))

def _cache_paths(cache_path: str) -> tuple[str, str]:
    base, ext = os.path.splitext(cache_path)
    ev_path = cache_path if ext.lower() == ".jsonl" else base + ".jsonl"
    meta_path = base + ".meta.json"
    return ev_path, meta_path

def _load_cache(cache_path: str) -> tuple[list[DepEvt], int]:
    """
    Returns (events, last_scanned_block). If no/invalid cache, returns ([], -1).
    """
    evts: list[DepEvt] = []
    last_scanned_block = -1
    ev_path, meta_path = _cache_paths(cache_path)
    try:
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
                last_scanned_block = int(meta.get("last_scanned_block", -1))
        if os.path.exists(ev_path):
            with open(ev_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    evts.append(_ev_from_dict(json.loads(line)))
    except Exception:
        evts, last_scanned_block = [], -1
    return evts, last_scanned_block

def _append_cache(cache_path: str, new_events: list[DepEvt], last_scanned_block: int) -> None:
    ev_path, meta_path = _cache_paths(cache_path)
    try:
        os.makedirs(os.path.dirname(ev_path) or ".", exist_ok=True)
        if new_events:
            with open(ev_path, "a") as f:
                for e in new_events:
                    f.write(json.dumps(_ev_to_dict(e)) + "\n")
        with open(meta_path, "w") as f:
            json.dump({"last_scanned_block": int(last_scanned_block)}, f)
    except Exception:
        # best-effort cache
        pass


# ----------------------
# Remainders per origin (event-level)
# ----------------------

def _per_origin_remainders_events(
    evts: List[DepEvt],
    cutoff_key: Tuple[int, int, int] | None
) -> tuple[int, int]:
    """
    Return (sum_remainders_before, sum_remainders_after_current) in WEI.
    Remainders are modulo 32 ETH per origin, computed over *event* amounts.
    cutoff_key is (block, txIndex, logIndex); events <= cutoff are "before".
    """
    modulus = ETH32_WEI
    by_origin: Dict[str, List[DepEvt]] = {}
    for e in evts:
        by_origin.setdefault(e.frm, []).append(e)

    before_sum = 0
    after_latest: Dict[str, int] = {}

    def is_before_or_equal(e: DepEvt) -> bool:
        if cutoff_key is None:
            return False
        return (e.block_number, e.tx_index, e.log_index) <= cutoff_key

    for origin, lst in by_origin.items():
        lst.sort(key=lambda x: (x.block_number, x.tx_index, x.log_index))
        cum = 0
        last_before = 0
        last_after = 0
        for e in lst:
            cum += e.amount_wei
            rem = cum % modulus
            if is_before_or_equal(e):
                last_before = rem
            else:
                last_after = rem
        before_sum += last_before
        after_latest[origin] = last_after

    return before_sum, sum(after_latest.values())


# ----------------------
# Public API
# ----------------------

def get_el_backlog_estimate_events(
    rpc_url: str,
    deposit_contract: str,
    processed_count: int,
    start_block_hint: int = 0,
    end_block: int = -1,  # -1 means "latest"
    block_stride: int = 10_000,  # Conservative default to avoid -32005
    incomplete_weight_after_cutoff: float = 0.5,
    cache_path: str | None = None,
    fallback_url: str | None = None,
    meta_cache_path: str | None = None
) -> tuple[int, int]:
    """
    Event-level backlog estimator.

    Args:
        rpc_url: Ethereum JSON-RPC URL
        deposit_contract: Deposit contract address
        processed_count: Number of deposits already processed
        start_block_hint: Earliest block to scan from
        end_block: Latest block to scan to (-1 for latest)
        block_stride: Block window size for log fetching
        incomplete_weight_after_cutoff: Weight for incomplete deposits after cutoff
        cache_path: Path to event cache file (JSONL format)
        fallback_url: Fallback RPC URL for external calls
        meta_cache_path: Path to metadata cache (block timestamps & tx from addresses).
                        Saves progress when rate limits are hit.

    Returns:
        (pending_events, pending_gwei)
    """
    log("=" * 60)
    log(f"Starting EL backlog estimation (processed_count={processed_count:,})")

    if processed_count < 0:
        processed_count = 0

    # 1) Load cache and decide scan start
    cached_evts: List[DepEvt] = []
    last_scanned_block = -1
    if cache_path:
        log(f"Loading cache from: {cache_path}")
        cached_evts, last_scanned_block = _load_cache(cache_path)
        log(f"Loaded {len(cached_evts)} cached events (last scanned block: {last_scanned_block:,})")

    scan_start = max(start_block_hint, last_scanned_block + 1)

    # 2) Fetch NEW logs only and hydrate
    log(f"Fetching new logs starting from block {scan_start:,}")
    try:
        raw_logs = list(_iter_deposit_logs(
            rpc_url=rpc_url,
            deposit_contract=deposit_contract,
            start_block=scan_start,
            end_block=end_block,
            block_stride=block_stride,
            topics=[DEPOSIT_EVENT_TOPIC0],
            fallback_url=fallback_url,
        ))
        log(f"Hydrating {len(raw_logs)} raw logs with metadata...")
        new_evts = _batch_hydrate_meta(fallback_url, raw_logs, fallback_url=fallback_url, meta_cache_path=meta_cache_path)
        log(f"Hydrated {len(new_evts)} new events")
    except (RpcError, HttpError) as e:
        # If we hit an error during fetching/hydration, we may have partial results
        # For now, we'll use what we had cached and re-raise
        error_type = "HTTP error" if isinstance(e, HttpError) else "RPC error"
        log(f"{error_type} during fetch/hydration. Using cached data only.")
        # We already saved metadata cache in _batch_hydrate_meta if applicable
        raise

    # 3) Merge and sort
    evts = cached_evts + new_evts
    if not evts:
        log("No events found, backlog is zero")
        return 0, 0
    evts.sort(key=lambda e: (e.block_number, e.tx_index, e.log_index))
    log(f"Total events: {len(evts)} (cached: {len(cached_evts)}, new: {len(new_evts)})")

    # 4) Persist cache (append only + meta)
    if cache_path:
        latest_block_seen = max(e.block_number for e in evts)
        log(f"Updating cache (latest block: {latest_block_seen:,})")
        _append_cache(cache_path, new_evts, latest_block_seen)

    # 5) Cutoff from processed_count (event index)
    cutoff_key: Optional[Tuple[int, int, int]] = None
    if processed_count > 0:
        idx = processed_count - 1
        if idx >= len(evts):
            # All processed
            log(f"All {len(evts)} events already processed, backlog is zero")
            return 0, 0
        e = evts[idx]
        cutoff_key = (e.block_number, e.tx_index, e.log_index)
        log(f"Cutoff at event index {processed_count-1} (block {e.block_number:,})")

    # 6) Sum amounts strictly after cutoff
    after_sum_wei = 0
    if cutoff_key is None:
        after_sum_wei = sum(e.amount_wei for e in evts)
        log(f"No cutoff, summing all {len(evts)} events")
    else:
        pending_count = 0
        for e in evts:
            key = (e.block_number, e.tx_index, e.log_index)
            if key > cutoff_key:
                after_sum_wei += e.amount_wei
                pending_count += 1
        log(f"Pending events after cutoff: {pending_count}")

    # 7) Per-origin remainders
    log("Calculating per-origin remainders...")
    rem_before_wei, rem_after_wei = _per_origin_remainders_events(evts, cutoff_key)
    log(f"Remainders: before={rem_before_wei//WEI_PER_GWEI:,} gwei, after={rem_after_wei//WEI_PER_GWEI:,} gwei")

    backlog_wei = after_sum_wei + rem_before_wei

    pending_events = backlog_wei // ETH32_WEI
    pending_gwei   = backlog_wei // WEI_PER_GWEI

    log(f"Final backlog: {pending_events:,} events, {pending_gwei:,} gwei")
    log("=" * 60)

    return int(pending_events), int(pending_gwei)


# ----------------------
# CLI (optional)
# ----------------------

if __name__ == "__main__":

    ev, gwei = get_el_backlog_estimate_events('http://192.168.1.132:8545',
                              deposit_contract =  '0x00000000219ab540356cBB839Cbe05303d7705Fa',
                              processed_count = 2045305,
                              start_block_hint = 0,
                              end_block = -1,
                              block_stride = 100000,
                              incomplete_weight_after_cutoff = 0.5,
                              cache_path= 'cache/deposit_events.cache.jsonl')
    print({"pending_events": ev, "pending_gwei": gwei})
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc-url", required=True, help="EL JSON-RPC URL (e.g. http://127.0.0.1:8545)")
    ap.add_argument("--deposit-contract", required=True)
    ap.add_argument("--processed-count", type=int, required=True)
    ap.add_argument("--start-block-hint", type=int, default=0)
    ap.add_argument("--end-block", type=int, default=-1, help="-1 means latest")
    ap.add_argument("--stride", type=int, default=100000)
    ap.add_argument("--weight", type=float, default=0.5)
    ap.add_argument("--cache", default=None, help="e.g. cache/deposit_events.cache.jsonl")
    args = ap.parse_args()

    ev, gwei = get_el_backlog_estimate_events(
        rpc_url=args.rpc_url,
        deposit_contract=args.deposit_contract,
        processed_count=args.processed_count,
        start_block_hint=args.start_block_hint,
        end_block=args.end_block,
        block_stride=args.stride,
        incomplete_weight_after_cutoff=args.weight,
        cache_path=args.cache,
    )
    print({"pending_events": ev, "pending_gwei": gwei})

