"""
Multi-provider LLM client for the local AdHocOnTap demos.

Auto-detects provider from environment variables, in this order:

  ANTHROPIC_API_KEY  -> Anthropic Claude  (paid, top quality)
  GEMINI_API_KEY     -> Google Gemini     (FREE tier ~1500 req/day)
  GROQ_API_KEY       -> Groq              (FREE tier, very fast Llama)
  OPENAI_API_KEY     -> OpenAI            (paid)
  (none of the above) -> Ollama localhost (FREE local, must be running)

Force a provider with DEFAULT_LLM_PROVIDER=anthropic|gemini|groq|openai|ollama
or by passing provider="..." to LLMClient().

API key signup links:
  Anthropic: https://console.anthropic.com/
  Gemini:    https://aistudio.google.com/app/apikey
  Groq:      https://console.groq.com/keys
  OpenAI:    https://platform.openai.com/api-keys
  Ollama:    https://ollama.com/   (run `ollama serve` then `ollama pull llama3.1`)
"""
from __future__ import annotations
import json
import os
from typing import Optional


# Default models per provider — picked for "fast + good enough for the demos"
DEFAULT_MODELS = {
    "anthropic": "claude-haiku-4-5-20251001",   # cheap & quick; bump to claude-sonnet-4-6 for richer narratives
    "gemini":    "gemini-2.0-flash",             # free tier
    "groq":      "llama-3.3-70b-versatile",      # free tier
    "openai":    "gpt-4o-mini",                  # cheap
    "ollama":    "llama3.1:8b",                  # local
}


class LLMClient:
    """Thin wrapper that exposes a single .chat(system, user) interface
    over any of the supported providers."""

    def __init__(self,
                 provider: Optional[str] = None,
                 model:    Optional[str] = None,
                 api_key:  Optional[str] = None,
                 max_tokens: int   = 4096,
                 temperature: float = 0.2):
        self.provider    = (provider or self._auto_detect_provider()).lower()
        self.model       = model or DEFAULT_MODELS.get(self.provider)
        self.api_key     = api_key or self._get_api_key()
        self.max_tokens  = max_tokens
        self.temperature = temperature
        self._client     = None
        self._init_client()

    # ── auto-detection ─────────────────────────────────────────
    @staticmethod
    def _auto_detect_provider() -> str:
        forced = os.environ.get("DEFAULT_LLM_PROVIDER")
        if forced:
            return forced
        if os.environ.get("ANTHROPIC_API_KEY"):
            return "anthropic"
        if os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"):
            return "gemini"
        if os.environ.get("GROQ_API_KEY"):
            return "groq"
        if os.environ.get("OPENAI_API_KEY"):
            return "openai"
        return "ollama"

    def _get_api_key(self) -> Optional[str]:
        if self.provider == "anthropic":
            return os.environ.get("ANTHROPIC_API_KEY")
        if self.provider == "gemini":
            return os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if self.provider == "groq":
            return os.environ.get("GROQ_API_KEY")
        if self.provider == "openai":
            return os.environ.get("OPENAI_API_KEY")
        return None  # ollama needs no key

    # ── client init ────────────────────────────────────────────
    def _init_client(self):
        if self.provider == "anthropic":
            try:
                import anthropic
            except ImportError:
                raise RuntimeError("Install: pip install anthropic")
            self._client = anthropic.Anthropic(api_key=self.api_key)

        elif self.provider == "gemini":
            try:
                from google import genai
            except ImportError:
                raise RuntimeError("Install: pip install google-genai")
            self._client = genai.Client(api_key=self.api_key)

        elif self.provider == "groq":
            try:
                from groq import Groq
            except ImportError:
                raise RuntimeError("Install: pip install groq")
            self._client = Groq(api_key=self.api_key)

        elif self.provider == "openai":
            try:
                from openai import OpenAI
            except ImportError:
                raise RuntimeError("Install: pip install openai")
            self._client = OpenAI(api_key=self.api_key)

        elif self.provider == "ollama":
            self._client = None  # plain HTTP

        else:
            raise ValueError(f"Unknown provider: {self.provider}")

    # ── main interface ─────────────────────────────────────────
    def chat(self, system: str, user: str,
             max_tokens: Optional[int] = None) -> str:
        max_tokens = max_tokens or self.max_tokens

        if self.provider == "anthropic":
            resp = self._client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=self.temperature,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            return resp.content[0].text

        if self.provider == "gemini":
            full = f"{system}\n\n{user}"
            resp = self._client.models.generate_content(
                model=self.model,
                contents=full,
                config={"temperature": self.temperature,
                        "max_output_tokens": max_tokens},
            )
            return resp.text

        if self.provider == "groq":
            resp = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                max_tokens=max_tokens,
                temperature=self.temperature,
            )
            return resp.choices[0].message.content

        if self.provider == "openai":
            resp = self._client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                max_tokens=max_tokens,
                temperature=self.temperature,
            )
            return resp.choices[0].message.content

        if self.provider == "ollama":
            import urllib.request
            body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                "stream": False,
                "options": {"temperature": self.temperature,
                            "num_predict": max_tokens},
            }
            req = urllib.request.Request(
                "http://localhost:11434/api/chat",
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=300) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["message"]["content"]

        raise RuntimeError(f"Provider {self.provider} not handled")

    def __repr__(self):
        return f"LLMClient(provider={self.provider}, model={self.model})"


def get_call_llm_fn(client: LLMClient):
    """Return a (system, user, max_tokens=None) -> str function bound to this
    client.  local_analyzer.analyze() expects a callable with this signature."""
    def call_llm(system: str, user: str, max_tokens=None) -> str:
        return client.chat(system, user, max_tokens=max_tokens)
    return call_llm
