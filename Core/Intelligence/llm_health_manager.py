# llm_health_manager.py: Adaptive LLM provider health-check and routing.
# Part of LeoBook Core — Intelligence (AI Engine)
#
# Classes: LLMHealthManager
# Called by: api_manager.py, build_search_dict.py
"""
Multi-key, multi-model LLM health manager.
- Grok: single key (GROK_API_KEY)
- Gemini: comma-separated keys (GEMINI_API_KEY=key1,key2,...,key14)
  Round-robins through active keys AND models to maximize free-tier quota.
Model Chains (Mar 2026 free-tier rate limits per key):
  gemini-2.5-pro 5 RPM / 100 RPD (best reasoning)
  gemini-3-flash 5 RPM / 20 RPD (frontier preview)
  gemini-2.5-flash 10 RPM / 250 RPD (balanced)
  gemini-2.0-flash 15 RPM / 1500 RPD (high throughput)
  gemini-2.5-flash-lite 15 RPM / 1000 RPD (cheap)
  gemini-3.1-flash-lite 15 RPM / 1000 RPD (cheapest, ultra-fast, 1M tokens)
DESCENDING = pro-first (AIGO predictions, match analysis)
ASCENDING = lite-first (search-dict metadata enrichment)
"""
import os
import time
import asyncio
import requests
import threading
from dotenv import load_dotenv
load_dotenv()
PING_INTERVAL = 900  # 15 minutes

# Daily quota reset window (seconds). Gemini free-tier resets at midnight Pacific.
# 24 hours used as a safe upper bound; daily-exhausted models stay marked for this long.
DAILY_QUOTA_WINDOW = 86400  # 24 hours


class LLMHealthManager:
    """Singleton manager with multi-key, multi-model Gemini rotation."""
    _instance = None
    _lock = asyncio.Lock()

    # ── Model Chains ──────────────────────────────────────────
    # DESCENDING: max intelligence first (AIGO / predictions)
    # gemini-2.5-flash-lite excluded — reserved for SearchDict
    MODELS_DESCENDING = [
        "gemini-2.5-pro",
        "gemini-3-flash-preview",   # REVERT: "gemini-3-flash" is not found by the native SDK generateContent endpoint
        "gemini-2.5-flash",
        "gemini-2.0-flash",
    ]
    # ASCENDING: max throughput first (search-dict / bulk enrichment)
    # gemini-2.5-pro excluded — reserved for AIGO
    MODELS_ASCENDING = [
        "gemini-3.1-flash-lite-preview",
        "gemini-2.5-flash-lite",
        "gemini-2.0-flash",
        "gemini-2.5-flash",
        "gemini-3-flash-preview",   # REVERT: "gemini-3-flash" is not found by the native SDK generateContent endpoint
    ]
    # Default model for health-check pings (cheapest)
    PING_MODEL = "gemini-3.1-flash-lite-preview"
    GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    GROK_API_URL = "https://api.x.ai/v1/chat/completions"
    GROK_MODEL = "grok-beta"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._grok_active = False
            cls._instance._gemini_keys = []      # All parsed keys
            cls._instance._gemini_active = []    # Keys that passed ping
            cls._instance._gemini_index = 0      # Round-robin pointer
            cls._instance._last_ping = 0.0
            cls._instance._initialized = False
            # Per-model per-minute cooldown: {model_name: {key: expiry_timestamp}}
            cls._instance._model_cooldowns = {}
            # Permanently dead keys (403/401/400) — persists across ping cycles
            cls._instance._dead_keys = set()
            # Per-model daily exhaustion: {model_name: timestamp_marked}
            # Auto-expires after DAILY_QUOTA_WINDOW. NOT reset by reset_model_exhaustion().
            cls._instance._model_daily_exhausted = {}
            # Default per-minute cooldown duration (Gemini free-tier resets ~60s)
            cls._instance.COOLDOWN_SECONDS = 65
            # Thread-safe lock for state mutations (get_next / on_429 / etc)
            cls._instance._state_lock = threading.Lock()
        return cls._instance

    # ── Public API ──────────────────────────────────────────────
    async def ensure_initialized(self):
        """Ping providers if we haven't yet or if the interval has elapsed."""
        now = time.time()
        if not self._initialized or (now - self._last_ping) >= PING_INTERVAL:
            async with self._lock:
                if not self._initialized or (time.time() - self._last_ping) >= PING_INTERVAL:
                    await self._ping_all()

    def get_ordered_providers(self) -> list:
        """Returns provider names ordered: active first, inactive last."""
        grok_configured = bool(os.getenv("GROK_API_KEY", "").strip())
        if not self._initialized:
            providers = ["Gemini"]
            if grok_configured:
                providers.insert(0, "Grok")
            return providers
        active = []
        inactive = []
        if grok_configured:
            if self._grok_active:
                active.append("Grok")
            else:
                inactive.append("Grok")
        if self._gemini_active:
            active.append("Gemini")
        else:
            inactive.append("Gemini")
        return active + inactive

    def is_provider_active(self, name: str) -> bool:
        """Check if a specific provider has at least one active key."""
        if name == "Grok":
            return self._grok_active
        if name == "Gemini":
            return len(self._gemini_active) > 0
        return False

    def get_model_chain(self, context: str = "aigo") -> list:
        """
        Returns the model priority chain for the given context.

        Args:
            context: "aigo" for DESCENDING (predictions/analysis),
                     "search_dict" for ASCENDING (bulk enrichment).
        """
        if context == "search_dict":
            return list(self.MODELS_ASCENDING)
        return list(self.MODELS_DESCENDING)

    def get_next_gemini_key(self, model: str = None) -> str:
        """
        Round-robin through active Gemini keys, skipping:
          - Keys on per-minute cooldown for the given model.
          - Models that are daily-exhausted (quota hit 0 for the day).
        Returns empty string if no key is available.
        """
        with self._state_lock:
            # Fast-fail: if the model itself is daily-exhausted, no key will work.
            if model and self._is_daily_exhausted_unlocked(model):
                return ""
            pool = self._gemini_active if self._gemini_active else self._gemini_keys
            if not pool:
                return ""
            now = time.time()
            cooldowns = self._model_cooldowns.get(model, {}) if model else {}
            # Prune expired per-minute cooldowns
            if cooldowns:
                expired = [k for k, exp in cooldowns.items() if exp <= now]
                for k in expired:
                    del cooldowns[k]
            available = [k for k in pool if k not in cooldowns]
            if not available:
                return ""
            key = available[self._gemini_index % len(available)]
            self._gemini_index += 1
            return key

    def get_cooldown_remaining(self, model: str) -> float:
        """Returns seconds until the earliest per-minute cooldown for this model expires. 0 if none."""
        with self._state_lock:
            cooldowns = self._model_cooldowns.get(model, {})
            if not cooldowns:
                return 0.0
            now = time.time()
            active = [exp for exp in cooldowns.values() if exp > now]
            if not active:
                return 0.0
            return min(active) - now

    def has_chain_capacity(self, chain_name: str = "aigo") -> bool:
        """
        Returns True if at least one model in the given chain has available keys
        (not per-minute-cooled AND not daily-exhausted).

        Use this as a pre-flight circuit-breaker before starting a batch LLM operation.
        Unlike is_provider_active(), this correctly returns False during quota exhaustion.
        """
        with self._state_lock:
            chain = self.get_model_chain(chain_name)
            pool = self._gemini_active if self._gemini_active else self._gemini_keys
            if not pool:
                return False
            now = time.time()
            for model_name in chain:
                if self._is_daily_exhausted_unlocked(model_name):
                    continue
                cooldowns = self._model_cooldowns.get(model_name, {})
                available = [k for k in pool if cooldowns.get(k, 0) <= now]
                if available:
                    return True
            return False

    def is_model_daily_exhausted(self, model: str) -> bool:
        """Public: check if a model's daily quota is known to be zero."""
        with self._state_lock:
            return self._is_daily_exhausted_unlocked(model)

    def on_gemini_429(self, failed_key: str, model: str = None, err_str: str = ""):
        """
        Called when a Gemini key hits 429 for a specific model.

        Distinguishes two 429 scenarios from err_str:
          - Per-day exhaustion ('PerDay' + 'limit: 0'): marks the MODEL as daily-exhausted.
            The model stays unavailable for DAILY_QUOTA_WINDOW seconds (24h).
          - Per-minute throttle (default): applies COOLDOWN_SECONDS cooldown to the key.
            The key auto-recovers after 65s.

        Args:
            failed_key: The API key that returned 429.
            model: The model string that was in use.
            err_str: Full exception/response string — used for daily-limit detection.
        """
        with self._state_lock:
            is_daily = self._detect_daily_limit(err_str)

            if is_daily and model:
                # Mark entire model as daily-exhausted — no key rotation will help.
                self._model_daily_exhausted[model] = time.time()
                print(
                    f" [LLM Health] Daily quota exhausted for {model} — "
                    f"model unavailable for ~24h."
                )
                return  # Per-minute cooldown is irrelevant; daily mark covers it.

            if model:
                if model not in self._model_cooldowns:
                    self._model_cooldowns[model] = {}
                expiry = time.time() + self.COOLDOWN_SECONDS
                self._model_cooldowns[model][failed_key] = expiry
                pool = self._gemini_active or self._gemini_keys
                now = time.time()
                remaining = len([
                    k for k in pool
                    if k not in self._model_cooldowns[model]
                    or self._model_cooldowns[model][k] <= now
                ])
                print(
                    f" [LLM Health] Key ...{failed_key[-4:]} cooling down for {self.COOLDOWN_SECONDS}s "
                    f"on {model}. {remaining} keys available for this model."
                )
                if remaining == 0:
                    print(f" [LLM Health] [!] All keys on cooldown for {model} -- waiting or downgrading.")
            else:
                # Legacy path (no model specified): remove key from active pool entirely.
                if failed_key in self._gemini_active:
                    self._gemini_active.remove(failed_key)
                    remaining = len(self._gemini_active)
                    print(f" [LLM Health] Gemini key rotated out (429). {remaining} keys remaining.")
                    if remaining == 0:
                        print(f" [LLM Health] [!] All {len(self._gemini_keys)} Gemini keys exhausted!")

    def on_gemini_fatal_error(self, failed_key: str, reason: str = "403/400/401"):
        """Called when a Gemini key hits a fatal error (400 Invalid, 401 Unauth, 403 Forbidden).
        Permanently removes key from ALL pools.
        """
        with self._state_lock:
            if failed_key in self._dead_keys:
                return
            self._dead_keys.add(failed_key)
            if failed_key in self._gemini_active:
                self._gemini_active.remove(failed_key)
            if failed_key in self._gemini_keys:
                self._gemini_keys.remove(failed_key)
            print(
                f" [LLM Health] Gemini key permanently removed ({reason}). "
                f"{len(self._gemini_active)} active, {len(self._gemini_keys)} total."
            )

    def reset_model_exhaustion(self):
        """Reset per-minute cooldown tracking (call at start of each cycle).

        NOTE: Does NOT reset daily exhaustion — those persist for DAILY_QUOTA_WINDOW.
        Call reset_daily_exhaustion() explicitly if needed (e.g., after a confirmed day rollover).
        """
        with self._state_lock:
            self._model_cooldowns.clear()

    def reset_daily_exhaustion(self):
        """Explicitly clear all daily exhaustion markers (e.g., on confirmed new day)."""
        with self._state_lock:
            self._model_daily_exhausted.clear()
            print(" [LLM Health] Daily exhaustion markers cleared.")

    # ── Internals ───────────────────────────────────────────────
    def _is_daily_exhausted_unlocked(self, model: str) -> bool:
        """Internal check — caller must hold _state_lock."""
        ts = self._model_daily_exhausted.get(model)
        if ts is None:
            return False
        if time.time() - ts >= DAILY_QUOTA_WINDOW:
            # Auto-expire — daily quota window has rolled over.
            del self._model_daily_exhausted[model]
            return False
        return True

    @staticmethod
    def _detect_daily_limit(err_str: str) -> bool:
        """
        Heuristic: returns True if a 429 error indicates per-day quota exhaustion.

        Gemini API signals this via 'PerDay' in the quotaId AND 'limit: 0' in the
        violation message text (meaning 0 daily requests remaining, not just throttled).
        Per-minute throttles show a non-zero limit value (e.g. 'limit: 5').
        """
        if not err_str:
            return False
        return "PerDay" in err_str and "limit: 0" in err_str

    async def _ping_all(self):
        """Ping Grok + sample Gemini keys."""
        print(" [LLM Health] Pinging providers...")
        # Parse Gemini keys — exclude permanently dead keys
        raw = os.getenv("GEMINI_API_KEY", "")
        self._gemini_keys = [
            k.strip() for k in raw.split(",")
            if k.strip() and k.strip() not in self._dead_keys
        ]
        # Reset per-minute cooldowns — rate limits have likely recovered since last ping.
        # Daily exhaustion is intentionally NOT reset here; it persists until
        # DAILY_QUOTA_WINDOW elapses naturally or reset_daily_exhaustion() is called.
        self.reset_model_exhaustion()
        # Ping Grok (only if key is configured)
        grok_key = os.getenv("GROK_API_KEY", "").strip()
        if grok_key:
            self._grok_active = await self._ping_key("Grok", self.GROK_API_URL, self.GROK_MODEL, grok_key)
            tag = "[OK] Active" if self._grok_active else "[X] Inactive"
            print(f" [LLM Health] Grok: {tag}")
        else:
            self._grok_active = False
        # Ping Gemini keys (sample 3 to avoid wasting quota)
        if self._gemini_keys:
            n = len(self._gemini_keys)
            sample_indices = [0]
            if n > 1:
                sample_indices.append(n // 2)
            if n > 2:
                sample_indices.append(n - 1)
            sample_indices = list(dict.fromkeys(sample_indices))  # deterministic + unique
            sample_results = []
            for idx in sample_indices:
                key = self._gemini_keys[idx]
                status = await self._ping_key("Gemini", self.GEMINI_API_URL, self.PING_MODEL, key)
                if status == "FATAL":
                    self.on_gemini_fatal_error(key, "Dead Key detected in ping")
                    sample_results.append(False)
                else:
                    sample_results.append(status == "OK")
            if any(sample_results):
                self._gemini_active = list(self._gemini_keys)
                # FIX: report total unique models across both chains, not just DESCENDING count
                all_models = set(self.MODELS_ASCENDING + self.MODELS_DESCENDING)
                print(
                    f" [LLM Health] Gemini: [OK] Active ({len(self._gemini_keys)} keys, "
                    f"{len(all_models)} models available)"
                )
            else:
                self._gemini_active = []
                print(f" [LLM Health] Gemini: [X] Inactive (all {len(self._gemini_keys)} keys failed)")
        else:
            self._gemini_active = []
            print(" [LLM Health] Gemini: [X] No keys configured")
        self._last_ping = time.time()
        self._initialized = True
        with self._state_lock:
            self._gemini_index = 0  # Reset round-robin pointer after fresh ping cycle
        if not self._grok_active and not self._gemini_active:
            print(" [LLM Health] [!] CRITICAL -- All LLM providers are offline! User action required.")

    async def _ping_key(self, name: str, api_url: str, model: str, api_key: str) -> str:
        """Ping a single API key. Returns 'OK' (200/429), 'FATAL' (401/403/400-Invalid), or 'FAIL'."""
        if not api_key:
            return "FAIL"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 5,
            "temperature": 0,
        }
        def _do_ping():
            try:
                resp = requests.post(api_url, headers=headers, json=payload, timeout=10)
                if resp.status_code in (401, 403) or (resp.status_code == 400 and "INVALID_ARGUMENT" in resp.text):
                    return "FATAL"
                return "OK" if resp.status_code in (200, 429) else "FAIL"
            except Exception:
                return "FAIL"
        return await asyncio.to_thread(_do_ping)


# Module-level singleton
health_manager = LLMHealthManager()