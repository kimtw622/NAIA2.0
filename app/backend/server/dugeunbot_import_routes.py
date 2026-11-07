"""Loopback-only prompt import from prompt.dugeunbot.com into desktop NAIA."""

from __future__ import annotations

import math
import os
import re
from typing import Any, Awaitable, Callable
from urllib.parse import quote, urlparse

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.headless_payload_utils import is_loopback_host
from core.web_session_context import WebSessionContext

try:
    from core.nai_model_contract import NAI_VALID_SAMPLERS
except Exception:  # 업스트림 상수가 사라져도 임포트 자체는 살아야 한다.
    NAI_VALID_SAMPLERS = frozenset({
        "k_euler_ancestral",
        "k_euler",
        "k_dpmpp_2s_ancestral",
        "k_dpmpp_2m_sde",
        "k_dpmpp_2m",
        "k_dpmpp_sde",
        "ddim",
        "ddim_v3",
    })

# NAI 모드 스케줄러 선택지(headless_session_state_service.scheduler_options_for_mode)와 동일.
NAI_IMPORT_SCHEDULERS = frozenset({"karras", "native", "exponential", "polyexponential"})


AsyncRunner = Callable[..., Awaitable[Any]]
JsonBroadcaster = Callable[[set[Any], dict[str, Any]], Awaitable[None]]
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{20,128}$")
DEFAULT_HANDOFF_ORIGIN = "https://api.dugeunbot.com"
# 두근봇 데스크톱 핸드오프(create_desktop_handoff)가 허용하는 상한과 같은 값이다.
# 봇이 이미 통과시킨 레시피를 여기서 더 짧게 자르면 임포트가 통째로 거절된다
# (_bounded_string은 자르지 않고 invalid_recipe를 던진다). NAIA EXIF 프롬프트는
# 긍정 1200자·부정 1700자를 예사로 넘으므로 봇 쪽 상한이 바뀌면 여기도 같이 올릴 것.
# 봇: services/novelai_image_service.MAX_PROMPT_CHARS / MAX_NEGATIVE_CHARS,
#     services/prompt_handoff_service.MAX_DESKTOP_CHARACTER_*
MAX_BASE_POSITIVE = 3000
MAX_BASE_NEGATIVE = 3000
MAX_CHARACTER_POSITIVE = 3000
MAX_CHARACTER_NEGATIVE = 3000
MAX_CHARACTER_NAME = 40
MAX_CHARACTERS = 6


class DugeunbotImportError(Exception):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _request_host(value: str) -> str:
    raw = str(value or "").strip().lower()
    if raw.startswith("[") and "]" in raw:
        return raw[1:raw.index("]")]
    if raw.count(":") == 1:
        return raw.rsplit(":", 1)[0]
    return raw


def _is_local_request(req: Request) -> bool:
    client_host = _request_host(getattr(req.client, "host", "") if req.client else "")
    request_host = _request_host(req.headers.get("host") or getattr(req.url, "hostname", ""))
    if client_host == "testclient":
        return request_host in {"", "testserver"} or is_loopback_host(request_host)
    return is_loopback_host(client_host) and is_loopback_host(request_host)


def _handoff_origin() -> str:
    raw = str(os.environ.get("NAIA_DUGEUNBOT_HANDOFF_ORIGIN") or DEFAULT_HANDOFF_ORIGIN).rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme == "https" and parsed.hostname:
        return raw
    if parsed.scheme == "http" and parsed.hostname and is_loopback_host(parsed.hostname):
        return raw
    raise DugeunbotImportError(
        "invalid_origin",
        "두근봇 연동 주소가 안전하지 않습니다.",
        status=500,
    )


def _bounded_string(value: Any, *, field: str, maximum: int, required: bool = False) -> str:
    if not isinstance(value, str):
        raise DugeunbotImportError("invalid_recipe", f"잘못된 레시피 필드: {field}")
    text = value.strip()
    if required and not text:
        raise DugeunbotImportError("invalid_recipe", f"필수 프롬프트가 비어 있습니다: {field}")
    if len(text) > maximum:
        raise DugeunbotImportError("invalid_recipe", f"프롬프트가 너무 깁니다: {field}")
    return text


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _generation_settings(raw: Any) -> dict[str, Any]:
    """EXIF 생성 설정을 NAIA remote_params 키로 옮긴다. 범위를 벗어나면 그 값만 버린다."""
    if not isinstance(raw, dict):
        return {}
    settings: dict[str, Any] = {}
    steps = _finite_number(raw.get("steps"))
    if steps is not None and 1 <= int(steps) <= 50:
        settings["steps"] = int(steps)
    scale = _finite_number(raw.get("scale"))
    if scale is not None and 0 < scale <= 30:
        settings["cfg_scale"] = scale
    rescale = _finite_number(raw.get("cfg_rescale"))
    if rescale is not None and 0 <= rescale <= 1:
        settings["cfg_rescale"] = rescale
    sampler = raw.get("sampler")
    if isinstance(sampler, str) and sampler.strip() in NAI_VALID_SAMPLERS:
        settings["sampler"] = sampler.strip()
    schedule = raw.get("noise_schedule")
    if isinstance(schedule, str) and schedule.strip().lower() in NAI_IMPORT_SCHEDULERS:
        settings["scheduler"] = schedule.strip().lower()
    return settings


def normalize_recipe_for_naia(recipe: Any) -> dict[str, Any]:
    """Defensively reduce a handoff recipe to fields NAIA is allowed to apply."""
    if not isinstance(recipe, dict):
        raise DugeunbotImportError("invalid_recipe", "레시피 형식이 올바르지 않습니다.")
    version = recipe.get("version", 1)
    if isinstance(version, bool) or version not in (1, 2):
        raise DugeunbotImportError("invalid_recipe", "지원하지 않는 레시피 버전입니다.")

    base = recipe.get("base") if version == 2 else recipe
    if not isinstance(base, dict):
        raise DugeunbotImportError("invalid_recipe", "기본 프롬프트 형식이 올바르지 않습니다.")
    normalized = {
        "positive": _bounded_string(
            base.get("positive"), field="positive", maximum=MAX_BASE_POSITIVE, required=True
        ),
        "negative": _bounded_string(
            base.get("negative", ""), field="negative", maximum=MAX_BASE_NEGATIVE
        ),
        "characters": [],
        # 태그빌더가 EXIF에서 읽어 함께 실은 생성 설정(CFG·스텝 등).
        # 사이트 토글이 꺼져 있으면 필드 자체가 없고 빈 dict가 된다.
        "generation": _generation_settings(recipe.get("generation")),
    }

    if version == 2:
        raw_characters = recipe.get("characters")
        if not isinstance(raw_characters, list) or not 1 <= len(raw_characters) <= MAX_CHARACTERS:
            raise DugeunbotImportError("invalid_recipe", "캐릭터 수는 1명부터 6명까지 가능합니다.")
        for index, raw in enumerate(raw_characters):
            if not isinstance(raw, dict):
                raise DugeunbotImportError("invalid_recipe", "캐릭터 프롬프트 형식이 올바르지 않습니다.")
            normalized["characters"].append({
                "name": _bounded_string(
                    raw.get("name", ""), field=f"characters[{index}].name", maximum=MAX_CHARACTER_NAME
                ),
                "positive": _bounded_string(
                    raw.get("positive"),
                    field=f"characters[{index}].positive",
                    maximum=MAX_CHARACTER_POSITIVE,
                    required=True,
                ),
                "negative": _bounded_string(
                    raw.get("negative", ""),
                    field=f"characters[{index}].negative",
                    maximum=MAX_CHARACTER_NEGATIVE,
                ),
            })
    return normalized


def fetch_handoff_recipe(token: str) -> dict[str, Any]:
    if not TOKEN_PATTERN.fullmatch(str(token or "")):
        raise DugeunbotImportError("invalid_token", "NAIA 가져오기 링크가 올바르지 않습니다.")
    url = f"{_handoff_origin()}/api/handoffs/naia/{quote(token, safe='')}"
    try:
        response = requests.get(
            url,
            timeout=(5, 10),
            headers={"Accept": "application/json", "User-Agent": "NAIA-Dugeunbot-Import/1"},
        )
    except requests.RequestException as exc:
        raise DugeunbotImportError(
            "handoff_unreachable",
            "두근봇 연동 서버에 연결하지 못했습니다.",
            status=502,
        ) from exc
    if response.status_code == 404:
        raise DugeunbotImportError(
            "token_expired",
            "가져오기 링크가 만료됐거나 이미 사용되었습니다.",
            status=410,
        )
    if response.status_code != 200:
        raise DugeunbotImportError(
            "handoff_failed",
            f"두근봇 연동 서버가 요청을 처리하지 못했습니다 ({response.status_code}).",
            status=502,
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise DugeunbotImportError(
            "invalid_response", "두근봇 연동 응답이 올바르지 않습니다.", status=502
        ) from exc
    return normalize_recipe_for_naia(payload.get("recipe") if isinstance(payload, dict) else None)


def apply_recipe_to_context(context: WebSessionContext, recipe: dict[str, Any]) -> dict[str, Any]:
    context.set_api_mode("NAI")
    context.prompt_text = recipe["positive"]
    context.negative_prompt_text = recipe["negative"]
    character_state = context._character_service().replace_characters(recipe["characters"])
    # EXIF 생성 설정 적용 — set_param이 타입 강제·상태 저장까지 처리한다.
    for key, value in (recipe.get("generation") or {}).items():
        context.set_param(key, value)
    context.save_remote_ui_state()
    return character_state


def register_dugeunbot_import_routes(
    app: FastAPI,
    session_context: WebSessionContext,
    *,
    clients: set[Any],
    run_in_thread: AsyncRunner,
    broadcast_json: JsonBroadcaster,
) -> None:
    @app.post("/api/integrations/dugeunbot/import")
    async def api_dugeunbot_import(req: Request):
        if not _is_local_request(req):
            return JSONResponse(
                {"ok": False, "error": "loopback_only"},
                status_code=403,
            )
        try:
            body = await req.json()
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)
        token = body.get("token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not TOKEN_PATTERN.fullmatch(token):
            return JSONResponse({"ok": False, "error": "invalid_token"}, status_code=400)
        try:
            recipe = await run_in_thread(fetch_handoff_recipe, token)
            character_state = apply_recipe_to_context(session_context, recipe)
        except DugeunbotImportError as exc:
            await broadcast_json(clients, {
                "type": "toast",
                "level": "error",
                "message": exc.message,
            })
            return JSONResponse(
                {"ok": False, "error": exc.code, "message": exc.message},
                status_code=exc.status,
            )
        except Exception:
            message = "프롬프트를 NAIA에 적용하지 못했습니다."
            await broadcast_json(clients, {"type": "toast", "level": "error", "message": message})
            return JSONResponse(
                {"ok": False, "error": "apply_failed", "message": message},
                status_code=500,
            )

        await broadcast_json(clients, {"type": "mode", "mode": "NAI"})
        await broadcast_json(clients, {
            "type": "prompt_sync",
            "prompt": session_context.prompt_text,
            "negative": session_context.negative_prompt_text,
            "negative_prompt": session_context.negative_prompt_text,
            "force": True,
        })
        # This import replaces the complete character collection. Mark the
        # broadcast so the web client can discard any debounced textarea edit
        # from the previously loaded image instead of recreating a stale slot.
        authoritative_character_state = dict(character_state)
        authoritative_character_state["authoritative_replace"] = True
        await broadcast_json(
            clients,
            session_context._module_state_payload("character", authoritative_character_state),
        )
        applied_params = recipe.get("generation") or {}
        if applied_params:
            # set_param으로 이미 반영된 값을 열려 있는 UI에도 동기화한다.
            await broadcast_json(clients, session_context.generation_param_schema_payload())
        count = len(recipe["characters"])
        detail = f" · 캐릭터 {count}명" if count else ""
        if applied_params:
            detail += " · 생성 설정 적용"
        await broadcast_json(clients, {
            "type": "toast",
            "level": "success",
            "message": f"바비 태그 프롬프트를 불러왔습니다{detail}.",
        })
        return {"ok": True, "mode": "NAI", "character_count": count, "applied_params": sorted(applied_params)}
