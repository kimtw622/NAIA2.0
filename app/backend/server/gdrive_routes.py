"""Google Drive 이미지 피커 라우트 (커스텀 패치, 제거 가능).

Remote Web의 이미지 업로드 지점(Character Reference / Vibe Transfer 업로드 바,
결과 뷰어 우클릭 "Google Drive에서 가져오기")에서 구글 드라이브의 사진을 바로
골라 올 수 있게 한다. 인증/전송은 호스트에 이미 설정된 **rclone** 리모트에
위임한다(OAuth 코드 없음). 서버 PC에 ``rclone`` 과 ``rclone config`` 로 만든
Drive 리모트(기본 이름 ``gdrive``)가 있어야 한다.

rclone은 매 호출마다 프로세스를 띄우면(``rclone cat``) Drive 경로 해석 때문에
파일 하나에 6~60초가 걸린다. 그래서 첫 사용 시 ``rclone rcd`` 데몬을 loopback
전용(랜덤 Basic auth)으로 하나 띄워 두고 ``operations/list`` + ``--rc-serve``
GET으로 재사용한다(디렉터리 캐시 공유 → 목록 ~0.6s, 파일 ~2s). 데몬은 앱 종료
시 함께 내린다.

엔드포인트:
- ``GET  /api/gdrive/status``            rclone 유무·리모트 목록·현재 설정
- ``POST /api/gdrive/config``            {remote, root} 저장 (config_dir/gdrive_settings.json)
- ``GET  /api/gdrive/list?path=``        폴더 목록(하위 폴더 + 이미지 파일)
- ``GET  /api/gdrive/thumb?path=&size=`` 썸네일(JPEG, cache_dir 캐시)
- ``POST /api/gdrive/fetch``             {path} → 원본 이미지 바이트

설정 우선순위: 환경변수 ``NAIA_RCLONE_BIN`` / ``NAIA_GDRIVE_REMOTE`` /
``NAIA_GDRIVE_ROOT`` > 설정 파일 > 기본값. 리모트 이름은 ``rclone listremotes``
에 있는 것만 허용하고, 경로에 ``..``/``:``/선행 ``/`` 는 거부해 다른 리모트나
상위 경로로 새지 않게 한다.

제거: 이 파일 + headless_routes 등록 1줄 + ``js/features/gdrivePicker.mjs`` +
app.js/imageModulePanels/resultContextMenu 연결 + style.css ``.gdrive-*``.
"""

from __future__ import annotations

import atexit
import base64
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import quote
from pathlib import Path, PurePosixPath
from typing import Any, Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

AsyncRunner = Callable[..., Awaitable[Any]]

SETTINGS_FILENAME = "gdrive_settings.json"
DEFAULT_REMOTE = "gdrive"
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".avif", ".heic", ".heif"}
LIST_TIMEOUT_SEC = 40
FETCH_TIMEOUT_SEC = 180
DAEMON_START_TIMEOUT_SEC = 15
THUMB_SIZE_MIN = 64
THUMB_SIZE_MAX = 512
THUMB_SIZE_DEFAULT = 192
THUMB_CACHE_MAX_FILES = 3000
# 썸네일은 요청마다 rclone 프로세스 + 원본 다운로드가 든다. 동시 실행을 제한한다.
_THUMB_SEMAPHORE = threading.BoundedSemaphore(4)

_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".avif": "image/avif",
    ".heic": "image/heic",
    ".heif": "image/heif",
}

_RCLONE_CANDIDATES = (
    "/opt/homebrew/bin/rclone",
    "/usr/local/bin/rclone",
    "/usr/bin/rclone",
    r"C:\Program Files\rclone\rclone.exe",
    r"C:\rclone\rclone.exe",
    # winget install Rclone.Rclone 이 만드는 링크 (사용자 PATH 갱신 전에도 찾히게)
    os.path.join(os.environ.get("LOCALAPPDATA", r"C:\Users\Default\AppData\Local"), "Microsoft", "WinGet", "Links", "rclone.exe"),
)


class GdriveError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


# ---------------------------------------------------------------------------
# 경로/설정 헬퍼
# ---------------------------------------------------------------------------

def _config_dir() -> Path:
    try:
        from app.backend.runtime.paths import resolve_runtime_paths

        path = resolve_runtime_paths().config_dir
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        fallback = Path(tempfile.gettempdir()) / "naia_config"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _cache_dir() -> Path:
    try:
        from app.backend.runtime.paths import resolve_runtime_paths

        path = resolve_runtime_paths().writable_dir("cache") / "gdrive_thumbs"
        path.mkdir(parents=True, exist_ok=True)
        return path
    except Exception:
        fallback = Path(tempfile.gettempdir()) / "naia_gdrive_thumbs"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _settings_path() -> Path:
    return _config_dir() / SETTINGS_FILENAME


def _load_settings() -> dict[str, Any]:
    try:
        raw = json.loads(_settings_path().read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_settings(data: dict[str, Any]) -> None:
    path = _settings_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _clean_root(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/").strip("/")
    if not text:
        return ""
    _validate_rel_path(text)
    return text


def _current_remote() -> str:
    env = os.environ.get("NAIA_GDRIVE_REMOTE", "").strip()
    if env:
        return env.rstrip(":")
    saved = str(_load_settings().get("remote") or "").strip().rstrip(":")
    return saved or DEFAULT_REMOTE


def _current_root() -> str:
    env = os.environ.get("NAIA_GDRIVE_ROOT")
    if env is not None and env.strip():
        return _clean_root(env)
    try:
        return _clean_root(_load_settings().get("root"))
    except GdriveError:
        return ""


def _rclone_bin() -> str | None:
    env = os.environ.get("NAIA_RCLONE_BIN", "").strip()
    if env and Path(env).exists():
        return env
    found = shutil.which("rclone")
    if found:
        return found
    for candidate in _RCLONE_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    # Electron 설치본(user-data/tools/rclone)·프로젝트 루트 tools/ 아래도 훑는다.
    try:
        from app.backend.runtime.paths import resolve_runtime_paths

        paths = resolve_runtime_paths()
        exe = "rclone.exe" if sys.platform.startswith("win") else "rclone"
        for base in (paths.user_root / "tools" / "rclone", paths.project_root / "tools" / "rclone"):
            candidate = base / exe
            if candidate.exists():
                return str(candidate)
    except Exception:
        pass
    return None


def _validate_rel_path(rel: str) -> str:
    text = str(rel or "").replace("\\", "/").strip()
    if text.startswith("/"):
        raise GdriveError("절대 경로는 사용할 수 없습니다")
    if ":" in text:
        raise GdriveError("경로에 ':'를 쓸 수 없습니다")
    parts = [p for p in text.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise GdriveError("상위 경로 이동은 허용되지 않습니다")
    if "\x00" in text:
        raise GdriveError("잘못된 경로입니다")
    return "/".join(parts)


def _subprocess_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if sys.platform.startswith("win"):
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return kwargs


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _RcloneDaemon:
    """``rclone rcd`` 상주 프로세스 관리자 (loopback 전용, 랜덤 Basic auth)."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._proc: subprocess.Popen | None = None
        self._port = 0
        self._user = "naia"
        self._password = ""
        self._binary = ""
        self._log_handle: Any = None

    # -- 수명 --
    def _alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None and self._port > 0

    def stop(self) -> None:
        with self._lock:
            proc = self._proc
            self._proc = None
            self._port = 0
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        if self._log_handle is not None:
            try:
                self._log_handle.close()
            except Exception:
                pass
            self._log_handle = None

    def _start_locked(self) -> None:
        binary = _rclone_bin()
        if not binary:
            raise GdriveError(
                "rclone을 찾을 수 없습니다. 서버 PC에 rclone을 설치하고 PATH에 두거나 NAIA_RCLONE_BIN을 지정하세요.",
                503,
            )
        port = _free_port()
        password = secrets.token_urlsafe(24)
        args = [
            binary, "rcd",
            "--rc-addr", f"127.0.0.1:{port}",
            "--rc-user", self._user,
            "--rc-pass", password,
            "--rc-serve",
            "--rc-job-expire-duration", "1m",
            "--log-level", "NOTICE",
        ]
        log_path = None
        try:
            log_path = _cache_dir().parent / "gdrive_rclone.log"
            self._log_handle = open(log_path, "ab")
            stdout = self._log_handle
        except Exception:
            self._log_handle = None
            stdout = subprocess.DEVNULL
        try:
            proc = subprocess.Popen(
                args,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=subprocess.STDOUT,
                **_subprocess_kwargs(),
            )
        except OSError as exc:
            raise GdriveError(f"rclone 실행 실패: {exc}", 500)
        self._proc = proc
        self._port = port
        self._password = password
        self._binary = binary
        deadline = time.monotonic() + DAEMON_START_TIMEOUT_SEC
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                self._proc = None
                self._port = 0
                raise GdriveError(f"rclone rcd가 바로 종료되었습니다 (exit {proc.returncode}). 로그: {log_path}", 500)
            try:
                self._post("rc/noop", {}, timeout=3)
                return
            except Exception as exc:  # 아직 기동 중
                last_error = exc
                time.sleep(0.2)
        self.stop()
        raise GdriveError(f"rclone rcd 기동 시간 초과: {last_error}", 504)

    def ensure(self) -> None:
        with self._lock:
            if self._alive():
                return
            self.stop()
            self._start_locked()

    # -- 호출 --
    def _auth_header(self) -> str:
        token = base64.b64encode(f"{self._user}:{self._password}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"

    def _base(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def _post(self, command: str, payload: dict[str, Any], *, timeout: int) -> dict[str, Any]:
        body = json.dumps(payload or {}).encode("utf-8")
        req = urllib.request.Request(
            f"{self._base()}/{command}",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": self._auth_header()},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = json.loads(exc.read().decode("utf-8", "replace")).get("error", "")
            except Exception:
                pass
            status = 404 if exc.code == 404 or "not found" in detail.lower() else 502
            raise GdriveError(f"rclone 오류: {detail or exc}", status)
        except urllib.error.URLError as exc:
            raise GdriveError(f"rclone 데몬 연결 실패: {exc.reason}", 502)
        except socket.timeout:
            raise GdriveError("rclone 응답 시간 초과", 504)
        try:
            data = json.loads(raw.decode("utf-8", "replace") or "{}")
        except Exception:
            raise GdriveError("rclone 응답을 해석하지 못했습니다", 502)
        if isinstance(data, dict) and data.get("error"):
            raise GdriveError(f"rclone 오류: {data.get('error')}", 502)
        return data if isinstance(data, dict) else {}

    def post(self, command: str, payload: dict[str, Any], *, timeout: int) -> dict[str, Any]:
        self.ensure()
        try:
            return self._post(command, payload, timeout=timeout)
        except GdriveError as exc:
            # 데몬이 죽었으면 한 번 재기동 후 재시도.
            if exc.status == 502 and "연결 실패" in str(exc):
                self.stop()
                self.ensure()
                return self._post(command, payload, timeout=timeout)
            raise

    def get_file(self, fs: str, remote_path: str, *, timeout: int) -> bytes:
        self.ensure()
        url = f"{self._base()}/[{quote(fs, safe=':')}]/{quote(remote_path, safe='/')}"
        req = urllib.request.Request(url, method="GET", headers={"Authorization": self._auth_header()})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            status = 404 if exc.code == 404 else 502
            raise GdriveError(f"rclone 오류: HTTP {exc.code}", status)
        except urllib.error.URLError as exc:
            raise GdriveError(f"rclone 데몬 연결 실패: {exc.reason}", 502)
        except socket.timeout:
            raise GdriveError("rclone 응답 시간 초과", 504)


_DAEMON = _RcloneDaemon()
atexit.register(_DAEMON.stop)


def _list_remotes() -> list[str]:
    data = _DAEMON.post("config/listremotes", {}, timeout=20)
    remotes = data.get("remotes") if isinstance(data, dict) else None
    return [str(r).rstrip(":") for r in (remotes or []) if str(r).strip()]


def _media_type_for(name: str) -> str:
    return _MEDIA_TYPES.get(PurePosixPath(name).suffix.lower(), "application/octet-stream")


def _is_image_name(name: str) -> bool:
    return PurePosixPath(name).suffix.lower() in IMAGE_EXTENSIONS


# ---------------------------------------------------------------------------
# Google Drive API (rclone 토큰 재사용) — 목록/썸네일/다운로드의 기본 경로
# ---------------------------------------------------------------------------

DRIVE_API = "https://www.googleapis.com/drive/v3"
DRIVE_FOLDER_MIME = "application/vnd.google-apps.folder"
DRIVE_SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
DRIVE_LIST_FIELDS = (
    "nextPageToken,files(id,name,mimeType,size,modifiedTime,thumbnailLink,hasThumbnail,"
    "shortcutDetails(targetId,targetMimeType))"
)
PATH_CACHE_TTL_SEC = 600
_ID_SAFE = re.compile(r"^[A-Za-z0-9_-]{8,}$")


def _parse_rfc3339(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        from datetime import datetime

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        # 파이썬 3.10 fromisoformat 은 소수점 7자리 이상을 못 읽는다 → 6자리로 자른다.
        m = re.match(r"^(.*\.\d{6})\d+([+-]\d\d:\d\d)$", text)
        if m:
            text = m.group(1) + m.group(2)
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


class _DriveApi:
    """rclone 설정의 OAuth 토큰으로 Drive v3 API를 직접 호출한다.

    토큰 갱신은 rclone에 맡긴다: 만료가 임박하면 데몬에 ``operations/about`` 을
    한 번 날려 rclone이 토큰을 갱신·저장하게 한 뒤 ``config/get`` 으로 다시 읽는다.
    """

    def __init__(self, daemon: _RcloneDaemon) -> None:
        self._daemon = daemon
        self._lock = threading.RLock()
        self._token: str = ""
        self._token_expiry: float = 0.0
        self._token_remote: str = ""
        self._cfg: dict[str, Any] = {}
        self._path_cache: dict[tuple[str, str, str], tuple[str, float]] = {}

    # -- 토큰 --
    def _read_config(self, remote: str) -> dict[str, Any]:
        cfg = self._daemon.post("config/get", {"name": remote}, timeout=15)
        if not isinstance(cfg, dict) or not cfg:
            raise GdriveError(f"rclone 리모트 '{remote}' 설정을 읽지 못했습니다", 502)
        if str(cfg.get("type") or "") != "drive":
            raise GdriveError(f"rclone 리모트 '{remote}'는 Google Drive 타입이 아닙니다 (type={cfg.get('type')})")
        return cfg

    def _ensure_token(self, remote: str, *, force: bool = False) -> str:
        with self._lock:
            now = time.time()
            if (
                not force
                and self._token
                and self._token_remote == remote
                and self._token_expiry - now > 60
            ):
                return self._token
            cfg = self._read_config(remote)
            raw = cfg.get("token")
            try:
                tok = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                tok = {}
            expiry = _parse_rfc3339(str(tok.get("expiry") or ""))
            if force or not tok.get("access_token") or expiry - now <= 60:
                # rclone이 토큰을 갱신해 설정에 저장하도록 가벼운 호출을 한 번 보낸다.
                try:
                    self._daemon.post("operations/about", {"fs": f"{remote}:"}, timeout=30)
                except GdriveError:
                    pass
                cfg = self._read_config(remote)
                raw = cfg.get("token")
                try:
                    tok = json.loads(raw) if isinstance(raw, str) else (raw or {})
                except Exception:
                    tok = {}
                expiry = _parse_rfc3339(str(tok.get("expiry") or ""))
            access = str(tok.get("access_token") or "")
            if not access:
                raise GdriveError("rclone 설정에서 Google Drive 토큰을 찾지 못했습니다", 502)
            self._token = access
            self._token_expiry = expiry or (now + 300)
            self._token_remote = remote
            self._cfg = cfg
            return access

    def _root_id(self) -> str:
        cfg = self._cfg or {}
        team = str(cfg.get("team_drive") or "").strip()
        if team:
            return team
        root = str(cfg.get("root_folder_id") or "").strip()
        return root or "root"

    def _drive_params(self) -> dict[str, str]:
        params = {"supportsAllDrives": "true"}
        team = str((self._cfg or {}).get("team_drive") or "").strip()
        if team:
            params.update({"corpora": "drive", "driveId": team, "includeItemsFromAllDrives": "true"})
        return params

    # -- HTTP --
    def _request(self, remote: str, url: str, params: dict[str, str] | None = None, *, timeout: int, raw: bool = False, _retry: bool = True) -> Any:
        token = self._ensure_token(remote)
        full = url
        if params:
            from urllib.parse import urlencode

            full = f"{url}?{urlencode(params)}"
        req = urllib.request.Request(full, method="GET", headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and _retry:
                self._ensure_token(remote, force=True)
                return self._request(remote, url, params, timeout=timeout, raw=raw, _retry=False)
            detail = ""
            try:
                detail = json.loads(exc.read().decode("utf-8", "replace")).get("error", {}).get("message", "")
            except Exception:
                pass
            status = 404 if exc.code == 404 else (429 if exc.code == 429 else 502)
            raise GdriveError(f"Google Drive API 오류 {exc.code}: {detail or exc.reason}", status)
        except urllib.error.URLError as exc:
            raise GdriveError(f"Google Drive 연결 실패: {exc.reason}", 502)
        except socket.timeout:
            raise GdriveError("Google Drive 응답 시간 초과", 504)
        if raw:
            return body
        try:
            return json.loads(body.decode("utf-8", "replace") or "{}")
        except Exception:
            raise GdriveError("Google Drive 응답을 해석하지 못했습니다", 502)

    # -- 경로 → 폴더 ID --
    @staticmethod
    def _q(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def resolve_folder(self, remote: str, path: str) -> str:
        self._ensure_token(remote)
        rel = _validate_rel_path(path)
        key = (remote, self._root_id(), rel)
        now = time.time()
        with self._lock:
            hit = self._path_cache.get(key)
            if hit and now - hit[1] < PATH_CACHE_TTL_SEC:
                return hit[0]
        current = self._root_id()
        acc: list[str] = []
        for seg in rel.split("/") if rel else []:
            acc.append(seg)
            sub_key = (remote, self._root_id(), "/".join(acc))
            with self._lock:
                hit = self._path_cache.get(sub_key)
            if hit and now - hit[1] < PATH_CACHE_TTL_SEC:
                current = hit[0]
                continue
            q = (
                f"name = '{self._q(seg)}' and '{current}' in parents and trashed = false and "
                f"(mimeType = '{DRIVE_FOLDER_MIME}' or mimeType = '{DRIVE_SHORTCUT_MIME}')"
            )
            params = {"q": q, "fields": "files(id,mimeType,shortcutDetails(targetId,targetMimeType))", "pageSize": "10", **self._drive_params()}
            data = self._request(remote, f"{DRIVE_API}/files", params, timeout=LIST_TIMEOUT_SEC)
            files = data.get("files") if isinstance(data, dict) else None
            folder_id = ""
            for f in files or []:
                if f.get("mimeType") == DRIVE_FOLDER_MIME:
                    folder_id = str(f.get("id") or "")
                    break
                sc = f.get("shortcutDetails") or {}
                if sc.get("targetMimeType") == DRIVE_FOLDER_MIME and sc.get("targetId"):
                    folder_id = str(sc.get("targetId"))
                    break
            if not folder_id:
                raise GdriveError(f"폴더를 찾을 수 없습니다: {'/'.join(acc)}", 404)
            current = folder_id
            with self._lock:
                self._path_cache[sub_key] = (current, now)
        with self._lock:
            self._path_cache[key] = (current, now)
        return current

    def remember_folder(self, remote: str, path: str, folder_id: str) -> None:
        if not folder_id or not _ID_SAFE.match(folder_id):
            return
        with self._lock:
            self._path_cache[(remote, self._root_id(), _validate_rel_path(path))] = (folder_id, time.time())

    # -- 동작 --
    def list_children(self, remote: str, folder_id: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page_token = ""
        for _ in range(20):  # 최대 20,000개 — 이미지 피커 용도엔 충분
            params = {
                "q": f"'{folder_id}' in parents and trashed = false",
                "fields": DRIVE_LIST_FIELDS,
                "pageSize": "1000",
                "orderBy": "folder,modifiedTime desc,name",
                **self._drive_params(),
            }
            if page_token:
                params["pageToken"] = page_token
            data = self._request(remote, f"{DRIVE_API}/files", params, timeout=LIST_TIMEOUT_SEC)
            files = data.get("files") if isinstance(data, dict) else None
            items.extend(f for f in (files or []) if isinstance(f, dict))
            page_token = str(data.get("nextPageToken") or "") if isinstance(data, dict) else ""
            if not page_token:
                break
        return items

    def download(self, remote: str, file_id: str) -> bytes:
        if not _ID_SAFE.match(file_id or ""):
            raise GdriveError("잘못된 파일 ID입니다")
        params = {"alt": "media", **self._drive_params()}
        return self._request(remote, f"{DRIVE_API}/files/{file_id}", params, timeout=FETCH_TIMEOUT_SEC, raw=True)

    def thumbnail_link(self, remote: str, file_id: str) -> str:
        if not _ID_SAFE.match(file_id or ""):
            raise GdriveError("잘못된 파일 ID입니다")
        data = self._request(remote, f"{DRIVE_API}/files/{file_id}", {"fields": "thumbnailLink,hasThumbnail", **self._drive_params()}, timeout=20)
        return str(data.get("thumbnailLink") or "") if isinstance(data, dict) else ""

    def fetch_url(self, url: str, *, timeout: int) -> bytes:
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            raise GdriveError(f"썸네일 다운로드 실패: HTTP {exc.code}", 502)
        except (urllib.error.URLError, socket.timeout) as exc:
            raise GdriveError(f"썸네일 다운로드 실패: {exc}", 502)


_DRIVE = _DriveApi(_DAEMON)


def _thumb_link_sized(link: str, size: int) -> str:
    if not link:
        return ""
    return re.sub(r"=s\d+(-c)?$", "", link) + f"=s{int(size)}"


# ---------------------------------------------------------------------------
# 동작
# ---------------------------------------------------------------------------

def _status() -> dict[str, Any]:
    binary = _rclone_bin()
    remote = _current_remote()
    payload: dict[str, Any] = {
        "available": False,
        "rclone_bin": binary or "",
        "remote": remote,
        "root": _current_root(),
        "remotes": [],
        "remote_locked": bool(os.environ.get("NAIA_GDRIVE_REMOTE", "").strip()),
        "root_locked": bool(os.environ.get("NAIA_GDRIVE_ROOT", "").strip()),
        "api": "",
        "error": "",
    }
    if not binary:
        payload["error"] = "rclone이 설치되어 있지 않습니다"
        return payload
    try:
        remotes = _list_remotes()
    except GdriveError as exc:
        payload["error"] = str(exc)
        return payload
    payload["remotes"] = remotes
    if remote not in remotes:
        payload["error"] = f"rclone 리모트 '{remote}'가 없습니다. 설정에서 리모트를 고르거나 `rclone config`로 만드세요."
        return payload
    payload["available"] = True
    try:
        _DRIVE._ensure_token(remote)
        payload["api"] = "drive"
    except GdriveError as exc:
        payload["api"] = "rclone"
        payload["api_note"] = f"Drive API 직접 호출 불가({exc}) — rclone 경로로 동작합니다(느림)"
    return payload


def _crumbs_for(rel: str) -> list[dict[str, str]]:
    crumbs: list[dict[str, str]] = []
    acc: list[str] = []
    for part in rel.split("/") if rel else []:
        acc.append(part)
        crumbs.append({"name": part, "path": "/".join(acc)})
    return crumbs


def _list_dir_rclone(remote: str, root: str, rel: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    joined = "/".join(p for p in (root, rel) if p)
    data = _DAEMON.post(
        "operations/list",
        {"fs": f"{remote}:", "remote": joined, "opt": {"noMimeType": True}},
        timeout=LIST_TIMEOUT_SEC,
    )
    raw = data.get("list") if isinstance(data, dict) else None
    folders: list[dict[str, Any]] = []
    images: list[dict[str, Any]] = []
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("Name") or PurePosixPath(str(entry.get("Path") or "")).name)
        if not name:
            continue
        size = entry.get("Size")
        item = {
            "name": name,
            "path": "/".join(p for p in (rel, name) if p),
            "id": str(entry.get("ID") or ""),
            "mod_time": str(entry.get("ModTime") or ""),
            "size": int(size) if isinstance(size, (int, float)) and size >= 0 else 0,
            "thumb": "",
        }
        if entry.get("IsDir"):
            folders.append(item)
        elif _is_image_name(name):
            images.append(item)
    return folders, images


def _list_dir_api(remote: str, root: str, rel: str, folder_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    full_rel = "/".join(p for p in (root, rel) if p)
    if folder_id and _ID_SAFE.match(folder_id):
        _DRIVE.remember_folder(remote, full_rel, folder_id)
        fid = folder_id
    else:
        fid = _DRIVE.resolve_folder(remote, full_rel)
    folders: list[dict[str, Any]] = []
    images: list[dict[str, Any]] = []
    for f in _DRIVE.list_children(remote, fid):
        name = str(f.get("name") or "")
        if not name:
            continue
        mime = str(f.get("mimeType") or "")
        fid_item = str(f.get("id") or "")
        if mime == DRIVE_SHORTCUT_MIME:
            sc = f.get("shortcutDetails") or {}
            mime = str(sc.get("targetMimeType") or "")
            fid_item = str(sc.get("targetId") or "")
            if not fid_item:
                continue
        size_raw = f.get("size")
        try:
            size = int(size_raw) if size_raw is not None else 0
        except (TypeError, ValueError):
            size = 0
        item = {
            "name": name,
            "path": "/".join(p for p in (rel, name) if p),
            "id": fid_item,
            "mod_time": str(f.get("modifiedTime") or ""),
            "size": size,
            "thumb": "",
        }
        if mime == DRIVE_FOLDER_MIME:
            folders.append(item)
            _DRIVE.remember_folder(remote, "/".join(p for p in (full_rel, name) if p), fid_item)
        elif mime.startswith("image/") or _is_image_name(name):
            if not _is_image_name(name) and mime.startswith("image/"):
                pass  # 확장자 없는 이미지도 허용
            item["thumb"] = _thumb_link_sized(str(f.get("thumbnailLink") or ""), 256) if f.get("hasThumbnail", True) else ""
            images.append(item)
    return folders, images


# 폴더 목록 단기 캐시. Drive thumbnailLink는 목록 호출마다 서명이 바뀌어 브라우저가
# 썸네일을 재사용하지 못하므로, 짧은 시간 안에 같은 폴더를 다시 열면 같은 응답을 돌려준다.
LIST_CACHE_TTL_SEC = 180
_LIST_CACHE: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
_LIST_CACHE_LOCK = threading.Lock()


def _list_dir(rel_path: str, folder_id: str = "", *, force: bool = False) -> dict[str, Any]:
    rel = _validate_rel_path(rel_path)
    remote = _current_remote()
    root = _current_root()
    cache_key = (remote, root, rel)
    if not force:
        with _LIST_CACHE_LOCK:
            hit = _LIST_CACHE.get(cache_key)
        if hit and time.time() - hit[0] < LIST_CACHE_TTL_SEC:
            return hit[1]
    api = "drive"
    note = ""
    try:
        folders, images = _list_dir_api(remote, root, rel, folder_id)
    except GdriveError as exc:
        if exc.status == 404:
            raise
        # Drive API 경로가 막히면(토큰/권한 등) rclone 경로로 폴백한다.
        api = "rclone"
        note = str(exc)
        folders, images = _list_dir_rclone(remote, root, rel)
    folders.sort(key=lambda x: x["name"].lower())
    images.sort(key=lambda x: x["mod_time"], reverse=True)
    result = {
        "path": rel,
        "crumbs": _crumbs_for(rel),
        "folders": folders,
        "images": images,
        "remote": remote,
        "root": root,
        "api": api,
        "note": note,
    }
    with _LIST_CACHE_LOCK:
        _LIST_CACHE[cache_key] = (time.time(), result)
        if len(_LIST_CACHE) > 200:
            oldest = sorted(_LIST_CACHE.items(), key=lambda kv: kv[1][0])[: len(_LIST_CACHE) - 200]
            for key, _ in oldest:
                _LIST_CACHE.pop(key, None)
    return result


def _fetch_bytes(rel_path: str, file_id: str = "") -> tuple[bytes, str, str]:
    rel = _validate_rel_path(rel_path)
    if not rel:
        raise GdriveError("파일 경로가 필요합니다")
    name = PurePosixPath(rel).name
    remote = _current_remote()
    root = _current_root()
    data = b""
    if file_id and _ID_SAFE.match(file_id):
        try:
            data = _DRIVE.download(remote, file_id)
        except GdriveError as exc:
            if exc.status == 404:
                raise
            data = b""
    if not data:
        if not _is_image_name(name):
            raise GdriveError("이미지 파일만 가져올 수 있습니다")
        joined = "/".join(p for p in (root, rel) if p)
        data = _DAEMON.get_file(f"{remote}:", joined, timeout=FETCH_TIMEOUT_SEC)
    if not data:
        raise GdriveError("빈 파일입니다", 404)
    media = _media_type_for(name)
    if media == "application/octet-stream":
        media = _sniff_image_type(data) or media
    if not media.startswith("image/"):
        raise GdriveError("이미지 파일만 가져올 수 있습니다", 415)
    return data, name, media


def _sniff_image_type(data: bytes) -> str:
    head = data[:16]
    if head.startswith(b"\x89PNG"):
        return "image/png"
    if head.startswith(b"\xff\xd8"):
        return "image/jpeg"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    if head.startswith(b"GIF8"):
        return "image/gif"
    if head.startswith(b"BM"):
        return "image/bmp"
    if head[4:12] in (b"ftypavif", b"ftypheic", b"ftypheix", b"ftypmif1"):
        return "image/avif" if head[4:12] == b"ftypavif" else "image/heic"
    return ""


def _prune_thumb_cache(cache_dir: Path) -> None:
    try:
        files = sorted(cache_dir.glob("*.jpg"), key=lambda p: p.stat().st_mtime)
    except Exception:
        return
    excess = len(files) - THUMB_CACHE_MAX_FILES
    for path in files[: max(0, excess)]:
        try:
            path.unlink()
        except Exception:
            pass


def _thumbnail(rel_path: str, size: int, mod_time: str, file_id: str = "") -> bytes:
    """서버 생성 썸네일(폴백). 보통은 프론트가 Drive thumbnailLink를 직접 쓴다."""
    size = max(THUMB_SIZE_MIN, min(THUMB_SIZE_MAX, int(size or THUMB_SIZE_DEFAULT)))
    rel = _validate_rel_path(rel_path)
    key = hashlib.sha1(f"{_current_remote()}|{_current_root()}|{rel}|{file_id}|{mod_time}|{size}".encode("utf-8")).hexdigest()
    cache_dir = _cache_dir()
    cached = cache_dir / f"{key}.jpg"
    if cached.exists():
        try:
            os.utime(cached, None)
        except Exception:
            pass
        return cached.read_bytes()
    with _THUMB_SEMAPHORE:
        if cached.exists():
            return cached.read_bytes()
        out = b""
        # 1) Drive 네이티브 썸네일 (빠름)
        if file_id and _ID_SAFE.match(file_id):
            try:
                link = _DRIVE.thumbnail_link(_current_remote(), file_id)
                if link:
                    raw = _DRIVE.fetch_url(_thumb_link_sized(link, size), timeout=30)
                    out = _reencode_thumb(raw, size)
            except GdriveError:
                out = b""
        # 2) 원본 내려받아 PIL 축소 (폴백)
        if not out:
            data, _name, _media = _fetch_bytes(rel, file_id)
            out = _reencode_thumb(data, size)
        try:
            tmp = cached.with_suffix(".tmp")
            tmp.write_bytes(out)
            os.replace(tmp, cached)
            _prune_thumb_cache(cache_dir)
        except Exception:
            pass
        return out


def _reencode_thumb(data: bytes, size: int) -> bytes:
    try:
        from PIL import Image, ImageOps

        with Image.open(io.BytesIO(data)) as img:
            img = ImageOps.exif_transpose(img)
            img.thumbnail((size, size))
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGBA")
                background = Image.new("RGB", img.size, (24, 22, 40))
                background.paste(img, mask=img.split()[-1])
                img = background
            elif img.mode == "L":
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=82, optimize=True)
            return buf.getvalue()
    except Exception as exc:
        raise GdriveError(f"썸네일 생성 실패: {exc}", 415)


# ---------------------------------------------------------------------------
# 라우트 등록
# ---------------------------------------------------------------------------

def register_gdrive_routes(
    app: FastAPI,
    context: Any,
    *,
    run_in_thread: AsyncRunner,
) -> None:
    def _error(exc: Exception) -> JSONResponse:
        if isinstance(exc, GdriveError):
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status)
        return JSONResponse({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status_code=500)

    @app.get("/api/gdrive/status")
    async def api_gdrive_status():
        try:
            payload = await run_in_thread(_status)
        except Exception as exc:
            return _error(exc)
        return JSONResponse({"ok": True, **payload})

    @app.post("/api/gdrive/config")
    async def api_gdrive_config(req: Request):
        try:
            payload = await req.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        try:
            settings = _load_settings()
            if "remote" in payload and not os.environ.get("NAIA_GDRIVE_REMOTE", "").strip():
                remote = str(payload.get("remote") or "").strip().rstrip(":")
                if remote:
                    remotes = await run_in_thread(_list_remotes)
                    if remote not in remotes:
                        raise GdriveError(f"'{remote}'는 rclone에 설정된 리모트가 아닙니다")
                    settings["remote"] = remote
                else:
                    settings.pop("remote", None)
            if "root" in payload and not os.environ.get("NAIA_GDRIVE_ROOT", "").strip():
                settings["root"] = _clean_root(payload.get("root"))
            await run_in_thread(_save_settings, settings)
            with _LIST_CACHE_LOCK:
                _LIST_CACHE.clear()
            status = await run_in_thread(_status)
        except Exception as exc:
            return _error(exc)
        return JSONResponse({"ok": True, **status})

    @app.get("/api/gdrive/list")
    async def api_gdrive_list(req: Request):
        rel = str(req.query_params.get("path") or "")
        folder_id = str(req.query_params.get("id") or "")
        force = str(req.query_params.get("refresh") or "") in {"1", "true"}
        try:
            payload = await run_in_thread(_list_dir, rel, folder_id, force=force)
        except Exception as exc:
            return _error(exc)
        return JSONResponse({"ok": True, **payload})

    @app.get("/api/gdrive/thumb")
    async def api_gdrive_thumb(req: Request):
        rel = str(req.query_params.get("path") or "")
        file_id = str(req.query_params.get("id") or "")
        mod_time = str(req.query_params.get("mtime") or "")
        try:
            size = int(req.query_params.get("size") or THUMB_SIZE_DEFAULT)
        except ValueError:
            size = THUMB_SIZE_DEFAULT
        try:
            data = await run_in_thread(_thumbnail, rel, size, mod_time, file_id)
        except Exception as exc:
            return _error(exc)
        return Response(
            content=data,
            media_type="image/jpeg",
            headers={"Cache-Control": "private, max-age=86400"},
        )

    @app.post("/api/gdrive/fetch")
    async def api_gdrive_fetch(req: Request):
        try:
            payload = await req.json()
        except Exception:
            payload = {}
        rel = ""
        file_id = ""
        if isinstance(payload, dict):
            rel = str(payload.get("path") or "")
            file_id = str(payload.get("id") or "")
        if not rel:
            rel = str(req.query_params.get("path") or "")
        if not file_id:
            file_id = str(req.query_params.get("id") or "")
        try:
            data, name, media_type = await run_in_thread(_fetch_bytes, rel, file_id)
        except Exception as exc:
            return _error(exc)
        return Response(
            content=data,
            media_type=media_type,
            headers={
                "Content-Disposition": f"inline; filename*=UTF-8''{quote(name)}",
                "X-NAIA-Filename": quote(name),
                "Cache-Control": "no-store",
            },
        )
