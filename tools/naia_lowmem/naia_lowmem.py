"""naia_lowmem - NAIA 백엔드가 검색 풀을 RAM 에 여러 벌 올리지 않게 하는 애드온.

설치 위치는 ``user-data\\runtime-env\\Lib\\site-packages`` (``naia_lowmem.pth`` 가 인터프리터
시작 때 이 모듈을 import 한다). ``resources\\`` 는 건드리지 않으므로 NAIA 자동 업데이트에도
남는다. 끄려면 환경변수 ``NAIA_LOWMEM=0`` 또는 ``naia_lowmem.pth`` 삭제.

무엇을 하나 (2026-10-09 실측, 풀 679만 행 = 실데이터 3.8GB, 백엔드 커밋 14.5GB 에서 출발):

1. 환경변수 기본값
   - ``OPENBLAS_NUM_THREADS=4``  : ``import numpy`` 가 코어 수(24)만큼 스레드 버퍼를 잡아
     786MB 를 먹는다. 4 로 묶으면 120MB.
   - ``ARROW_DEFAULT_MEMORY_POOL=system`` : 기본 풀(mimalloc)은 해제한 메모리를 OS 에 돌려주지
     않아 임시 복사본이 전부 커밋으로 남는다.
2. ``core.parquet_chunk_loader.read_parquet_chunked`` : 큰 parquet 은 비압축 Arrow IPC 사본
   (``user-data\\cache\\lowmem\\*.arrow``)으로 한 번 풀어 두고 **메모리 매핑**으로 연다. 문자열
   열이 파일에 그대로 얹혀 프로세스 전용 메모리를 쓰지 않는다. 두 번째 실행부터는 parquet
   디코딩도 건너뛴다.
3. ``SearchResultModel._iter_bucketed_frames`` : 버킷이 행 순서대로 이어져 있으면(아카이브 검색
   결과는 항상 그렇다) 버킷마다 복사(take)하지 않고 슬라이스로 나눈다. 행마다의 버킷 번호도 100만
   행씩 나눠 매긴다(통째로 매기면 id 열만 한 임시 배열이 서너 벌 생긴다 - 820만 행 +0.2GB).
4. ``search_runtime.filter_source_frame`` : 걸러낼 행이 하나도 없으면 전체 복사를 건너뛴다.
5. ``search_runtime._build_tags_text`` : 매핑된 풀의 Tag Filter 색인(풀과 거의 같은 크기)도
   IPC 사본으로 만들어 매핑한다.
6. ``HeadlessSearchStateService.restore_last_search`` · ``search_runtime.run_search_command`` :
   풀을 새로 앉힐 때(시작 때 복원 · 새 검색) snapshot · master_base 를 ``.copy()`` 로 두 번 깊은
   복사한다(숫자 열 272MB x 2). pandas 3 은 항상 copy-on-write 라 얕은 사본도 서로 독립이므로 그
   호출만 ``copy(deep=False)`` 로 바꾼다. 본문을 베끼지 않고 **업스트림 소스의 그 호출만 고쳐 다시
   컴파일**한다.
7. ``search_runtime._dedup_by_id`` : 새 검색 · parquet 불러오기 뒤 중복 id 를 지울 때, 중복이 한
   건이라도 있으면 풀 전체를 다시 만든다(아카이브 전체 검색: 820만 행 중 7행 때문에 5GB 를 복사,
   순간 15.7GB). 버릴 행 사이의 구간을 슬라이스로 이어 붙여 같은 결과를 복사 없이 낸다.
8. 새 검색(``run_search_command``)의 결과를 **풀에 앉히기 전에** 매핑본으로 바꾼다. 검색 결과는
   아카이브 조각에서 RAM 으로 읽어 온 프레임이라, 그대로 앉히면 그 실행 내내 RAM 에 남는다(820만
   행 = 5.4GB; 2 번의 매핑은 시작 때 복원 경로에만 걸린다). 중복 제거 직후 그 프레임을 비압축
   Arrow IPC 사본으로 쓰고 매핑해, 값 · dtype · 열 · 행 순서가 같은 프레임을 대신 넘긴다. 풀에
   앉기 전이라 snapshot 객체 · 풀 세대 번호는 순정과 똑같이 한 번만 바뀐다.
   ``HeadlessSearchStateService.persist_last_search`` 가 last-search parquet 을 쓴 직후에는 그
   사본의 이름을 parquet 의 키(size · mtime_ns)로 바꿔, 다음 시작 때 2 번이 변환 없이 그대로 쓴다.
9. ``search_runtime.search_tag_archive_frame`` : 새 검색의 아카이브 스캔은 조각 175개의 결과를 전부
   RAM 에 모은 뒤 이어 붙인다 - 8 번이 그 뒤에 매핑하므로 검색 **도중**의 최고치는 그대로였다(820만
   행 = 5.76GB). 조각 결과를 차례가 되는 대로 8 번의 사본에 곧장 흘려 쓰고 버려, 풀 전체가 RAM 에
   한 번도 올라오지 않게 한다. 행 순서 · 숫자 열의 형(조각마다 달라 순정은 넓은 쪽으로 맞춘다) ·
   중복 id 제거(쓰기 전에 거른다) · 진행률 콜백은 순정과 같다. 한꺼번에 푸는 조각은 워커 수(8)가
   아니라 4개(검색어가 있으면 3개)까지만 둔다 - 그 수가 스캔 중의 순간 최고치이고, 더 돌려도 빨라지지
   않는다. 중복을 이미 거른 그 결과에 대해서는 뒤따르는 중복 검사(id 를 통째로 해시에 넣는다)도 건너뛴다.
10. ``search_runtime.save_runner_parquet`` : runner parquet(``naia_temp_rows.parquet``)을 쓸 때 풀
    모델이 버킷을 이어 붙이며 숫자 열을 한 벌 더 만든다(820만 행 +0.33GB = 새 검색의 순간 최고점).
    버킷 표를 그대로 이어 넘겨 **같은 바이트의 파일**을 그 복사 없이 쓴다. 새 검색은 last-search
    parquet 을 쓴 바로 뒤에 runner 를 또 쓰는데(820만 행 각 12초), 남은 풀이 last-search 와 행 · 순서까지
    같으면 인코딩하지 않고 그 파일을 복사하고(1초), 순서가 다르면(아카이브 전체 검색이 그렇다)
    last-search 를 쓰는 동안 다른 스레드에서 runner 를 미리 써 두었다가 차례가 오면 옮겨 넣는다. 기록은
    업스트림 ``SearchPoolWriter.write_now`` 가 그대로 하고, 그 안의 ``_atomic_to_parquet`` 만 감싼다.
11. ``SearchEngine._build_tags_string`` : 검색어 · 제외어가 있는 검색은 조각마다 태그 열 다섯을 파이썬
    문자열로 바꿔 쉼표로 이어 붙인 뒤 그것을 다시 Arrow 로 옮겨 매칭한다 - 파이썬 쪽 일이라 워커가 여덟이어도
    한 번에 하나만 돌고(스캔 22~35초), 문자열 수백만 개가 힙을 조각내 검색 뒤에도 0.2~0.4GB 가 남는다.
    Arrow 에 얹힌 태그 열을 Arrow 안에서 이어 붙여(``binary_join_element_wise``) 행마다 같은 문자열을
    파이썬 객체 없이 만든다. 그 열은 매칭에만 쓰이고 지워지므로 검색 결과는 순정과 같다.
12. ``search_runtime.filter_source_frame``(4 번과 같은 래퍼) : 이미 앉은 풀 안에서 검색어 · 제외어로 좁힐 때
    (심층검색 [필터] · 아카이브가 없을 때의 검색) 업스트림은 풀 전체의 태그 문자열을 한 번에 만들어 매칭하고,
    살아남은 행을 고르며 문자열 열을 통째로 이어 붙인다(820만 행 풀에서 순간 10~17GB). 업스트림
    ``_apply_filters`` 를 2.5만 행 구간마다 불러 살아남은 자리만 모으고, 그 뒤의 등급 · 태그 id 조건과 인덱스는
    순정이 하는 차례 그대로 재현한다. 결과가 크면 그 문자열 열을 디스크 사본에 얹어 돌려준다(사본 파일은 매핑한
    직후 지운다 - 결과가 버려지면 공간도 돌아온다).
13. ``depth_search_commands._handle_depth_action`` : 심층검색 [열기]가 풀을 두 층(원본 · 현재)으로 깊은 복사하는
    ``base.copy()`` 네 번을 6 번처럼 얕은 사본으로 고쳐 다시 컴파일한다(숫자 열 두 벌, 820만 행 +0.66GB).
    v2.0.58 부터 업스트림은 본문을 이 이름으로 옮기고 ``handle_depth_action`` 은 ``pool_busy`` 로 감싸 부르기만 한다.

안전장치: 패치마다 대상 함수의 소스 해시를 검사한다. NAIA 업데이트로 그 함수가 바뀌었으면
그 패치만 건너뛰고(= 순정 동작) ``cache\\lowmem\\status.json`` 에 사유를 남긴다. 그때는
``test_lowmem.py`` 로 동등성을 다시 확인한 뒤 ``EXPECTED`` 해시를 갱신한다
(``python -m naia_lowmem <naia-backend 경로>`` 가 현재 해시를 출력한다).
모든 패치는 예외가 나면 원래 함수로 넘어간다.
"""

from __future__ import annotations

import os
import sys
import threading

__version__ = "7"

_OFF = {"0", "false", "no", "off"}
ENABLED = os.environ.get("NAIA_LOWMEM", "1").strip().lower() not in _OFF
# 새 검색 결과를 매핑본으로 바꿔 앉힐지(8 번). 끄면 새로 검색한 풀은 재시작 전까지 RAM 에 남는다.
MAP_NEW_SEARCH = os.environ.get("NAIA_LOWMEM_MAP_SEARCH", "1").strip().lower() not in _OFF
# 새 검색의 스캔 결과를 조각 단위로 사본에 흘려 쓸지(9 번). 끄면 8 번만 - 스캔 결과를 RAM 에 다
# 모았다가 한꺼번에 사본으로 옮긴다(검색 도중 최고치가 풀 크기만큼 올라간다).
STREAM_NEW_SEARCH = os.environ.get("NAIA_LOWMEM_STREAM_SEARCH", "1").strip().lower() not in _OFF
# runner parquet 을 풀을 이어 붙이지 않고 쓰고, 새 검색 직후 같은 내용이면 last-search 파일을 복사할지
# (10 번). 끄면 순정대로 풀을 이어 붙여(숫자 열 한 벌 더) 매번 인코딩한다.
LEAN_RUNNER = os.environ.get("NAIA_LOWMEM_RUNNER", "1").strip().lower() not in _OFF
# 새 검색이 last-search parquet 을 쓰는 동안 runner parquet 을 다른 스레드에서 미리 써 둘지(10 번의 (다)).
# 끄면 runner 는 last-search 를 다 쓴 뒤에 차례로 쓴다(시간만 더 든다).
DRAFT_RUNNER = os.environ.get("NAIA_LOWMEM_RUNNER_DRAFT", "1").strip().lower() not in _OFF
# 검색어 · 제외어가 있는 검색에서 태그 문자열을 Arrow 로 이어 붙일지(11 번). 끄면 순정대로 파이썬 문자열로
# 만든다(스캔이 서너 배 느리고, 검색 뒤에 힙이 0.2~0.4GB 남는다).
ARROW_TAGS = os.environ.get("NAIA_LOWMEM_ARROW_TAGS", "1").strip().lower() not in _OFF
# 이미 앉은 풀 안에서 검색어 · 제외어로 좁힐 때(심층검색 · 불러온 셋 안의 검색) 풀을 구간마다 매칭할지(12 번).
# 끄면 순정대로 풀 전체의 태그 문자열을 한꺼번에 만든다(820만 행 풀에서 순간 10~17GB).
PIECE_FILTER = os.environ.get("NAIA_LOWMEM_PIECE_FILTER", "1").strip().lower() not in _OFF
# 그렇게 좁힌 결과가 크면 그 문자열 열을 디스크 사본에 얹어 돌려줄지(12 번). 끄면 결과는 RAM 에 둔다.
MAP_REFINE = os.environ.get("NAIA_LOWMEM_MAP_REFINE", "1").strip().lower() not in _OFF

# 이 크기보다 작은 parquet 은 매핑하지 않는다(어차피 가볍고, 사본만 늘어난다).
MIN_SOURCE_BYTES = 32 * 1024 * 1024
# 새 검색 결과(RAM 에서의 크기)가 이보다 작으면 RAM 에 둔다. parquet 은 실데이터의 1/3 쯤이라
# 위 기준(parquet 32MB)과 같은 풀 크기다.
MIN_FRAME_BYTES = 96 * 1024 * 1024
# 사본 폴더 상한. 넘으면 지금 쓰지 않는 오래된 사본부터 지운다.
MAX_CACHE_BYTES = 24 * 1024 ** 3
# Tag Filter 색인을 한 번에 만드는 행 수(= 색인 빌드 중 임시 메모리 상한). 원본 배치(10만)의 배수.
TAGS_TEXT_WINDOW = 800_000
# 새 검색 스캔(9 번): 스캔 중이거나 스캔을 마치고 쓸 차례를 기다리는 조각 수의 상한(워커 수를 넘지 않는다).
# 조각 하나를 푸는 동안 그 조각만 한 임시본이 서너 벌 생기므로(읽은 표 · pandas 로 옮긴 것 · 걸러 낸 것,
# 검색어가 있으면 태그 문자열까지 - 조각당 100MB 남짓) 이 수가 곧 스캔 중의 순간 최고치다.
# 검색어가 없으면 워커(8)를 다 돌려도 빨라지지 않는다(사본 쓰기가 병목). 2026-10-10 실측(아카이브 전체):
#   검색어 없음(820만 행): 8 = 스캔 5.6~7.8초 · 최고 0.90~0.99GB / 4 = 6.0초 · 0.61~0.65GB / 3 = 6.8초 / 2 = 9.9초
# 검색어가 있을 때는 11 번(태그 문자열을 Arrow 로) 뒤로 조각들이 정말로 함께 돈다 - 창을 넓히면 빨라지고
# 그만큼 최고치가 오른다. 3 이 꺾이는 자리다(2 는 눈에 띄게 느리고, 4 부터는 1초 안팎을 얻자고 0.1GB 씩
# 더 쓴다). 같은 날 밤 실측(스캔 시간 · 프로세스 최고치):
#   `hatsune miku`(9만 행):  2 = 12.3초 · 0.48GB / 3 = 6.7~7.7초 · 0.53GB / 4 = 6.8초 · 0.64GB
#                            / 6 = 5.1초 · 0.80GB / 8 = 4.3초 · 0.96GB
#   `1girl, solo`(499만 행): 2 = 15.9초 · 0.48GB / 3 = 10.3~10.8초 · 0.56~0.58GB / 4 = 9.1초 · 0.69GB
#                            / 6 = 7.7초 · 0.81GB / 8 = 6.7초 · 1.02GB
# (11 번 전에는 태그 문자열 조립이 파이썬 쪽이라 한 번에 한 스레드만 돌았다: 창이 3 이든 8 이든 22~35초.)
SCAN_WINDOW = 4
SCAN_WINDOW_WITH_QUERY = 3
# 새 검색 스캔의 중복 id 는 id 를 자리로 쓰는 bool 배열로 가린다(id 최댓값 1바이트씩 - 1천만이면
# 10MB). id 가 이 범위(0 이상 · 이 값 미만의 정수)를 벗어나면 흘려 쓰지 않는다.
ID_BITMAP_LIMIT = 1 << 27
# 사본을 쓰는 동안 남겨 둘 디스크 여유.
DISK_RESERVE_BYTES = 2 * 1024 ** 3
# 남은 풀이 이보다 적으면 runner parquet 은 순정 함수가 쓴다(10 번) - 이어 붙여도 가볍다(20만 행 = 8MB).
MIN_RUNNER_ROWS = 200_000
# 12 번: 풀 안에서 좁힐 때 한 번에 매칭하는 행 수와, 함께 매칭하는 구간 수. 구간 하나에 태그 문자열(행당 500바이트
# 남짓)과 살아남은 행의 그 문자열이 잠깐 생기므로 이 둘의 곱이 매칭 중의 순간 최고치다. 매칭은 Arrow 안에서 GIL 을
# 놓고 돌아 구간을 함께 돌릴수록 빨라진다. 2026-10-21 실측(820만 행 풀, 심층검색 [필터] 단계의 시간 · 그 단계 최고치):
#   `hatsune miku`: 5만 x 1 = 12.1초 · 1.27GB / 5만 x 3 = 4.2~4.6초 · 1.33~1.36GB / 5만 x 6 = 2.5초 · 1.41GB
#                   / 2.5만 x 6 = 2.2~2.3초 · 1.29~1.30GB
#   `1girl, solo` : 5만 x 1 = 18.8초 / 5만 x 3 = 8.5~9.8초 · 1.50GB / 2.5만 x 6 = 6.2~6.4초 · 1.50GB
REFINE_PIECE_ROWS = 25_000
REFINE_WORKERS = 6

# 대상 함수의 소스 해시(sha256 앞 16자). 검증한 NAIA 버전: v2.0.53 - v2.0.65.
EXPECTED = {
    "core.parquet_chunk_loader:read_parquet_chunked": {"6f9a5a2635368a4f"},
    "core.search_result_model:SearchResultModel._iter_bucketed_frames": {"abb7d7d743f476ad"},
    # 위 패치가 구간별로 나눠 부른다(감싸지 않는다). 바뀌었으면 통째로 한 번만 부른다.
    "core.search_result_model:SearchResultModel._bucket_ids_for_frame": {"19cdf27b18e467ce"},
    "app.backend.server.search_runtime:filter_source_frame": {"d108e97ae316833d"},
    "app.backend.server.search_runtime:_build_tags_text": {"0ea74603b5336618"},
    "core.headless_search_state_service:HeadlessSearchStateService.restore_last_search": {"0465fbd18056256c"},
    "core.headless_search_state_service:HeadlessSearchStateService.persist_last_search": {"7395bc16287f2984"},
    "app.backend.server.search_runtime:run_search_command": {"b20fa6fd2c9232c6"},
    # v2.0.58 부터 위 함수에 붙은 데코레이터(호출 내내 `pool_busy` 를 잡는다). 소스 해시는 def 줄부터라 데코레이터가
    # 들지 않는다 - 재작성할 때 그대로 다시 붙이므로 그 정의를 따로 확인한다(`_KNOWN_DECORATORS`).
    "app.backend.server.search_runtime:_pool_operation": {"23cba6d07c96efb4"},
    "app.backend.server.search_runtime:_dedup_by_id": {"85105e3de0c7c555"},
    "app.backend.server.search_runtime:search_tag_archive_frame": {"81c8b4846e87fbdf"},
    # 10 번(runner parquet): 첫 둘을 감싸고, 나머지 넷은 그 본문에 기대므로 같이 확인한다.
    "app.backend.server.search_runtime:save_runner_parquet": {"17ccea4e4ac6af26"},
    "core.search_pool_writer:_atomic_to_parquet": {"0940f243ce20ed4e"},
    "core.search_pool_writer:SearchPoolWriter.write_now": {"683e150ee9a4f620"},
    "core.custom_parquet_library:write_parquet": {"c4f4d7dd184ad9b8"},
    "core.search_result_model:SearchResultModel.get_dataframe": {"175fd5c5a41632b8"},
    "core.search_result_model:SearchResultModel._remaining_dataframe": {"38b06ad1064ba8ab"},
    # 11 번(태그 문자열): 첫째를 감싸고, 나머지 둘(과 위의 filter_source_frame)은 그 열을 받아 쓰는 쪽이라 같이 확인한다.
    "core.search_engine:SearchEngine._build_tags_string": {"a83a3295d91bbb7a"},
    "core.search_engine:SearchEngine._apply_filters": {"c12243d3ec3792fd"},
    "core.search_engine:SearchEngine.search_in_file": {"fcf1d58947ddcddc"},
    # 13 번(심층검색 [열기]의 `base.copy()` 네 번). v2.0.65 - v2.0.57 의 `handle_depth_action` 본문과 이름만 다르다.
    "app.backend.server.depth_search_commands:_handle_depth_action": {"6937d1f652988e68"},
}

ORIGINALS: dict = {}   # 패치 전 원본 함수 (테스트 · 진단용)
STATUS: dict = {"version": __version__, "patches": {}, "events": []}
# 지금 매핑해 둔 풀(최근 것 몇 개): [{"path": str, "table": pa.Table, "pending": bool}].
# pending = 새 검색이 만든 사본으로, 아직 parquet 과 짝지어지지 않아 이번 실행에서만 쓰인다(8 번).
_MAPPED: list = []
_MAPPED_KEEP = 3
_CONVERT_LOCK = threading.RLock()
# 새 검색이 지금 쓰고 있는 사본(9 번). 아직 _MAPPED 에 없어서, 정리(_prune)가 지우지 않게 따로 적어 둔다.
_WRITING: set = set()


if ENABLED:
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "4")
    os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")


# ── 진단 ─────────────────────────────────────────────────────────────────────
def _cache_dir():
    root = os.environ.get("NAIA_USER_DATA_DIR")
    if not root:
        return None
    return os.path.join(root, "cache", "lowmem")


def _log(message: str) -> None:
    try:
        print(f"naia_lowmem: {message}", flush=True)
    except Exception:
        pass


def _event(message: str) -> None:
    import time

    STATUS["events"].append(f"{time.strftime('%H:%M:%S')} {message}")
    del STATUS["events"][:-40]
    _log(message)
    _write_status()


def _write_status() -> None:
    directory = _cache_dir()
    if not directory:
        return
    try:
        import json
        import time

        os.makedirs(directory, exist_ok=True)
        payload = dict(STATUS)
        payload.update({
            "pid": os.getpid(),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "env": {k: os.environ.get(k) for k in ("OPENBLAS_NUM_THREADS", "ARROW_DEFAULT_MEMORY_POOL")},
        })
        tmp = os.path.join(directory, f"status.json.{os.getpid()}.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        os.replace(tmp, os.path.join(directory, "status.json"))
    except Exception:
        pass


# ── 소스 해시 ────────────────────────────────────────────────────────────────
def function_fingerprint(path: str, qualname: str) -> str:
    """파일 ``path`` 안 ``qualname``(예: ``Class.method``) 정의부의 sha256 앞 16자."""
    import ast
    import hashlib

    with open(path, encoding="utf-8") as handle:
        text = handle.read().replace("\r\n", "\n")
    node = ast.parse(text)
    for part in qualname.split("."):
        node = next(
            child for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and child.name == part
        )
    segment = ast.get_source_segment(text, node) or ""
    return hashlib.sha256(segment.encode("utf-8")).hexdigest()[:16]


def _guard_source(key: str, locate, qualname: str) -> bool:
    try:
        found = function_fingerprint(locate(), qualname)
    except Exception as exc:
        STATUS["patches"][key] = f"skipped: cannot fingerprint ({exc})"
        return False
    if found not in EXPECTED.get(key, set()):
        STATUS["patches"][key] = f"skipped: upstream changed (found {found})"
        return False
    STATUS["patches"][key] = "active"
    return True


def _guard(module, qualname: str) -> bool:
    """대상 함수가 검증한 그 소스일 때만 True. 아니면 사유를 남기고 False(= 순정 동작)."""
    return _guard_source(f"{module.__name__}:{qualname}", lambda: module.__file__, qualname)


def _guard_named(module_name: str, qualname: str) -> bool:
    """``_guard`` 와 같되 모듈을 이름으로 찾는다. 아직 올라오지 않은 모듈은 올리지 않고 소스 파일만 읽는다
    (업스트림이 필요할 때 올리는 순서를 바꾸지 않는다)."""

    def locate():
        module = sys.modules.get(module_name)
        if module is not None:
            return module.__file__
        import importlib.util

        return importlib.util.find_spec(module_name).origin

    return _guard_source(f"{module_name}:{qualname}", locate, qualname)


# ── 2. 큰 parquet 을 메모리 매핑으로 ─────────────────────────────────────────
def _sidecar_path(source: str, stat=None):
    directory = _cache_dir()
    if not directory:
        return None
    if stat is None:
        stat = os.stat(source)
    stem = os.path.splitext(os.path.basename(source))[0]
    return os.path.join(directory, f"{stem}-{stat.st_size:x}-{stat.st_mtime_ns:x}.arrow")


def _prune(directory: str, keep: set) -> None:
    """같은 원본의 옛 사본과, 상한을 넘긴 오래된 사본을 지운다. 열려 있는 파일은 건너뛴다."""
    try:
        keep_stems = {os.path.basename(path).split("-", 1)[0] for path in keep}
        keep_prefixes = {os.path.basename(path)[: -len(".arrow")] for path in keep}
        entries = []
        for name in os.listdir(directory):
            full = os.path.join(directory, name)
            if not os.path.isfile(full) or full in keep or full in _WRITING:
                continue
            if name.endswith(".tmp"):
                stale = True                       # 중단된 변환의 잔해
            elif name.endswith(".arrow"):
                stale = (name.split("-", 1)[0] in keep_stems
                         and not any(name.startswith(prefix) for prefix in keep_prefixes))
            else:
                continue
            if stale:
                try:
                    os.remove(full)
                except OSError:
                    pass
                continue
            entries.append((os.path.getmtime(full), os.path.getsize(full), full))
        total = sum(size for _, size, _ in entries) + sum(os.path.getsize(p) for p in keep if os.path.exists(p))
        for _, size, full in sorted(entries):
            if total <= MAX_CACHE_BYTES:
                break
            try:
                os.remove(full)
                total -= size
            except OSError:
                pass
    except Exception:
        pass


def _mappable_schema(schema):
    """pandas 3 의 문자열 배열은 large_string 이다. string 으로 두면 열 때마다 통째로 변환(복사)된다."""
    import pyarrow as pa

    fields = [
        field.with_type(pa.large_string()) if pa.types.is_string(field.type) else field
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _convert_to_ipc(source: str, sidecar: str, batch_rows: int, tick) -> None:
    """parquet → 비압축 Arrow IPC 파일. 배치 단위로 흘려 써서 풀 전체를 메모리에 올리지 않는다."""
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(source)
    metadata = parquet_file.metadata
    total = int(metadata.num_rows)
    needed = sum(
        metadata.row_group(index).total_byte_size for index in range(metadata.num_row_groups)
    )
    os.makedirs(os.path.dirname(sidecar), exist_ok=True)
    if shutil.disk_usage(os.path.dirname(sidecar)).free < needed * 1.3 + 2 * 1024 ** 3:
        raise OSError("not enough free disk space for the mapped copy")

    schema = _mappable_schema(parquet_file.schema_arrow)
    tmp = f"{sidecar}.{os.getpid()}.tmp"
    loaded = 0
    try:
        with pa.OSFile(tmp, "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
            for batch in parquet_file.iter_batches(batch_size=batch_rows):
                if batch.schema != schema:
                    batch = batch.cast(schema)
                writer.write_batch(batch)
                loaded += batch.num_rows
                tick(loaded, total or loaded)
        if loaded != total:
            raise OSError(f"row count mismatch while converting ({loaded} != {total})")
        os.replace(tmp, sidecar)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _open_mapped(sidecar: str):
    import pyarrow as pa

    return pa.ipc.open_file(pa.memory_map(sidecar, "r")).read_all()


def _remember(sidecar: str, table, *, pending: bool = False) -> dict:
    _MAPPED[:] = [entry for entry in _MAPPED if entry["path"] != sidecar]
    entry = {"path": sidecar, "table": table, "pending": pending}
    _MAPPED.append(entry)
    del _MAPPED[:-_MAPPED_KEEP]
    return entry


def _read_mapped(path, progress, batch_rows, columns):
    """매핑으로 읽을 수 있으면 DataFrame, 아니면 None(= 원래 방식으로 읽는다)."""
    if columns or not isinstance(path, (str, os.PathLike)):
        return None
    source = os.fspath(path)
    if not source.lower().endswith(".parquet") or not os.path.isfile(source):
        return None
    if os.path.getsize(source) < MIN_SOURCE_BYTES:
        return None
    sidecar = _sidecar_path(source)
    if sidecar is None:
        return None

    def tick(loaded: int, total: int) -> None:
        if progress is None:
            return
        try:
            progress(loaded, total)
        except Exception:
            pass

    import pyarrow.parquet as pq

    total = int(pq.ParquetFile(source).metadata.num_rows)
    with _CONVERT_LOCK:
        # 이미 매핑해 둔 사본이면 그 매핑을 다시 쓴다. 새로 매핑하면 주소가 달라져, 먼저 나간
        # 프레임을 Tag Filter 색인 쪽(_mapped_source_of)이 알아보지 못한다.
        table = next((entry["table"] for entry in _MAPPED if entry["path"] == sidecar), None)
        if table is not None and table.num_rows != total:
            table = None
        if table is not None:
            tick(0, total)
            tick(total, total)
            _remember(sidecar, table)
            return table.to_pandas().reset_index(drop=True)
        if os.path.exists(sidecar):
            try:
                table = _open_mapped(sidecar)
                if table.num_rows != total:
                    table = None
            except Exception:
                table = None
            if table is None:
                try:
                    os.remove(sidecar)
                except OSError:
                    pass
        if table is not None:
            tick(0, total)
            tick(total, total)
            _event(f"pool mapped from existing copy ({total:,} rows): {os.path.basename(sidecar)}")
        else:
            tick(0, total)
            _convert_to_ipc(source, sidecar, batch_rows, tick)
            table = _open_mapped(sidecar)
            if table.num_rows != total:
                raise OSError("mapped copy row count mismatch")
            _event(f"pool converted + mapped ({total:,} rows): {os.path.basename(sidecar)}")
        _remember(sidecar, table)
        _prune(os.path.dirname(sidecar), {entry["path"] for entry in _MAPPED} | _tags_sidecars_in_use())

    frame = table.to_pandas()
    return frame.reset_index(drop=True)


def _patch_parquet_chunk_loader(module) -> None:
    if not _guard(module, "read_parquet_chunked"):
        return
    original = module.read_parquet_chunked
    ORIGINALS["read_parquet_chunked"] = original
    default_rows = getattr(module, "DEFAULT_BATCH_ROWS", 100_000)

    def read_parquet_chunked(path, *, progress=None, batch_rows=default_rows, columns=None):
        try:
            frame = _read_mapped(path, progress, batch_rows, columns)
        except Exception as exc:
            frame = None
            _event(f"mapped read failed, using the stock reader - {exc}")
        if frame is not None:
            return frame
        return original(path, progress=progress, batch_rows=batch_rows, columns=columns)

    read_parquet_chunked.__doc__ = original.__doc__
    module.read_parquet_chunked = read_parquet_chunked


# ── 3. 버킷 분할을 복사 없이 ────────────────────────────────────────────────
# 버킷 하나가 이보다 많은 구간으로 흩어져 있으면(= 행이 버킷 순서로 놓여 있지 않다) 원본 방식에 맡긴다.
MAX_RUNS_PER_BUCKET = 4
# 행마다의 버킷 번호를 한 번에 매기는 행 수(= 그 계산의 임시 메모리 상한, 100만 행에 30MB 남짓).
BUCKET_ID_ROWS = 1_000_000


def _bucket_runs(np, bucket_ids):
    """행 순서대로 이어진 구간으로 버킷을 나눈다 → [(bucket_id, [(start, stop), ...])] (첫 등장 순).

    None 을 돌려주면 원본 방식(take)에 맡긴다: 버킷이 하나뿐이거나(원본이 이미 복사 없이 처리),
    구간이 너무 잘게 흩어져 있을 때."""
    if bucket_ids is None:
        return None
    count = len(bucket_ids)
    if count == 0:
        return None
    change = np.flatnonzero(bucket_ids[1:] != bucket_ids[:-1]) + 1
    if len(change) == 0:
        return None
    starts = np.concatenate(([0], change))
    stops = np.concatenate((change, [count]))
    run_ids = bucket_ids[starts]
    if len(run_ids) > MAX_RUNS_PER_BUCKET * len(np.unique(run_ids)):
        return None
    grouped: dict = {}
    for bucket_id, start, stop in zip(run_ids.tolist(), starts.tolist(), stops.tolist()):
        grouped.setdefault(int(bucket_id), []).append((int(start), int(stop)))
    if any(len(spans) > MAX_RUNS_PER_BUCKET for spans in grouped.values()):
        return None
    return list(grouped.items())


def _bucket_ids(np, cls, frame, piecewise: bool):
    """업스트림 ``_bucket_ids_for_frame(frame)`` 과 같은 값(행마다 버킷 번호).

    그 함수는 id 열만 한 임시 배열을 서너 벌 만든다(820만 행 +0.2GB, 풀을 앉힐 때마다). 버킷 번호는 행마다
    따로 정해지므로, BUCKET_ID_ROWS 행씩 잘라 그 함수를 부르고 결과(int16)만 이어 붙인다."""
    rows = len(frame)
    if not piecewise or rows <= BUCKET_ID_ROWS:
        return cls._bucket_ids_for_frame(frame)
    pieces = []
    for start in range(0, rows, BUCKET_ID_ROWS):
        piece = cls._bucket_ids_for_frame(frame.iloc[start:start + BUCKET_ID_ROWS])
        if piece is None:
            return None                # id 열이 없다 · 버킷 경계를 모른다 - 어느 구간이든 같다
        pieces.append(piece)
    return np.concatenate(pieces)


def _patch_search_result_model(module) -> None:
    if not _guard(module, "SearchResultModel._iter_bucketed_frames"):
        return
    np = module.np
    pd = module.pd
    cls = module.SearchResultModel
    original = cls.__dict__["_iter_bucketed_frames"].__func__
    ORIGINALS["_iter_bucketed_frames"] = original
    # 버킷 번호를 구간별로 매겨도 되는 것은 그 함수가 행마다 따로 계산하는, 확인한 그 소스일 때뿐이다.
    piecewise = _guard(module, "SearchResultModel._bucket_ids_for_frame")

    def _iter_bucketed_frames(cls, dataframe):
        try:
            frame = dataframe.reset_index(drop=True)
            runs = _bucket_runs(np, _bucket_ids(np, cls, frame, piecewise))
        except Exception:
            runs = None
        if runs is None:
            yield from original(cls, dataframe)
            return
        # 원본은 `frame.loc[bucket == id]`(행 복사)다. 같은 행 · 같은 순서를 슬라이스로 얻는다 -
        # arrow 열은 버퍼를 공유하고 숫자 열은 뷰라 새 메모리를 쓰지 않는다. 버킷이 두세 구간으로
        # 나뉘어 있으면 그 슬라이스들을 이어 붙인다(arrow 열은 청크만 늘고 복사는 없다).
        for bucket_id, spans in runs:
            if len(spans) == 1:
                start, stop = spans[0]
                yield bucket_id, frame.iloc[start:stop].reset_index(drop=True)
            else:
                yield bucket_id, pd.concat(
                    [frame.iloc[start:stop] for start, stop in spans], ignore_index=True
                )

    cls._iter_bucketed_frames = classmethod(_iter_bucketed_frames)


# ── 4. 걸러낼 게 없으면 복사하지 않는다 ─────────────────────────────────────
def _patch_filter_source_frame(module) -> None:
    if not _guard(module, "filter_source_frame"):
        return
    original = module.filter_source_frame
    ORIGINALS["filter_source_frame"] = original
    try:
        import pandas as pd

        # pandas 3 은 항상 copy-on-write 라 얕은 사본도 원본과 독립이다(고치는 쪽만 그때 복사된다).
        # 그 전 버전에서는 얕은 사본이 원본을 같이 고치므로 깊은 사본을 쓴다.
        deep = int(str(pd.__version__).split(".", 1)[0]) < 3
    except Exception:
        deep = True

    # 12 번(검색어 · 제외어가 있으면 구간마다 매칭)은 업스트림 검색 엔진의 두 함수에 기댄다 - 구간마다 그대로 부른다.
    pieces = (
        _copy_on_write()
        and _guard_named("core.search_engine", "SearchEngine._apply_filters")
        and _guard_named("core.search_engine", "SearchEngine._build_tags_string")
    )

    def filter_source_frame(frame, *, query="", exclude="", ratings=None, tag_ids=None):
        try:
            if (
                frame is not None
                and not getattr(frame, "empty", True)
                and not query
                and not exclude
                and tag_ids is None
                and (
                    not ratings
                    or "rating" not in frame.columns
                    or bool(frame["rating"].isin(ratings).all())
                )
            ):
                # 원본은 여기서 전부 True 인 마스크로 `frame[mask].copy()` = 풀 전체 복사를 한다.
                return frame.copy(deep=deep)
        except Exception:
            pass
        if (query or exclude) and pieces and PIECE_FILTER:
            try:
                result = _filter_in_pieces(frame, query, exclude, ratings, tag_ids)
            except Exception as exc:
                result = None
                _refine_note(str(exc) or type(exc).__name__)
            if result is not None:
                return result
        return original(frame, query=query, exclude=exclude, ratings=ratings, tag_ids=tag_ids)

    filter_source_frame.__doc__ = original.__doc__
    module.filter_source_frame = filter_source_frame


# ── 5. Tag Filter 색인도 매핑으로 ───────────────────────────────────────────
def _same_buffers(series, column) -> bool:
    """``series`` 가 매핑된 ``column`` 을 **그대로**(같은 버퍼 · 같은 구간) 들고 있는가."""
    chunked = getattr(series.array, "_pa_array", None)
    if chunked is None or chunked.num_chunks != column.num_chunks or len(chunked) != len(column):
        return False
    for mine, theirs in zip(chunked.chunks, column.chunks):
        if len(mine) != len(theirs) or mine.offset != theirs.offset:
            return False
        mine_buffers, their_buffers = mine.buffers(), theirs.buffers()
        if len(mine_buffers) != len(their_buffers):
            return False
        for a, b in zip(mine_buffers, their_buffers):
            if (a is None) != (b is None):
                return False
            if a is not None and (a.address != b.address or a.size != b.size):
                return False
    return True


def _mapped_source_of(frame, tag_columns):
    """``frame`` 의 태그 열이 전부 매핑된 풀 그대로면 그 풀의 사본 경로, 아니면 None."""
    for entry in reversed(_MAPPED):
        table = entry["table"]
        if table.num_rows != len(frame):
            continue
        names = set(table.column_names)
        if all(name in names and _same_buffers(frame[name], table.column(name)) for name in tag_columns):
            return entry["path"]
    return None


_TAGS_IN_USE: set = set()


def _tags_sidecars_in_use() -> set:
    return set(_TAGS_IN_USE)


def _tags_sidecar_path(pool_sidecar: str, tag_columns) -> str:
    import hashlib

    digest = hashlib.sha256("\x1f".join(tag_columns).encode("utf-8")).hexdigest()[:10]
    return f"{pool_sidecar[: -len('.arrow')]}.tags-{digest}.arrow"


def _patch_build_tags_text(module) -> None:
    if not _guard(module, "_build_tags_text"):
        return
    original = module._build_tags_text
    ORIGINALS["_build_tags_text"] = original
    batch = int(getattr(module, "_TAGS_TEXT_BATCH", 100_000))
    window = max(batch, (TAGS_TEXT_WINDOW // batch) * batch)

    def _build_mapped(frame, tag_columns, heartbeat, sidecar):
        import pyarrow as pa

        rows = len(frame)
        with _CONVERT_LOCK:
            if os.path.exists(sidecar):
                try:
                    column = _open_mapped(sidecar).column(0)
                    if len(column) == rows:
                        _TAGS_IN_USE.add(sidecar)
                        _event(f"tag index mapped from existing copy ({rows:,} rows)")
                        return column
                except Exception:
                    pass
                try:
                    os.remove(sidecar)
                except OSError:
                    pass
            schema = pa.schema([pa.field("tags_text", pa.large_string())])
            tmp = f"{sidecar}.{os.getpid()}.tmp"
            try:
                with pa.OSFile(tmp, "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
                    # 색인 내용은 원본 함수가 만든다. 풀을 창(window)으로 잘라 넘길 뿐이라 결과 ·
                    # 청크 경계(10만 행)가 통째로 만든 것과 같고, 임시 메모리는 창 하나로 묶인다.
                    for start in range(0, rows, window):
                        part = original(frame.iloc[start:start + window], tag_columns, heartbeat=None)
                        for chunk in part.chunks:
                            writer.write_batch(pa.record_batch([chunk], schema=schema))
                        del part
                        if heartbeat is not None:
                            try:
                                heartbeat(min(start + window, rows), rows)
                            except Exception:
                                pass
                os.replace(tmp, sidecar)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
            column = _open_mapped(sidecar).column(0)
            if len(column) != rows:
                raise OSError("mapped tag index row count mismatch")
            _TAGS_IN_USE.add(sidecar)
            _event(f"tag index built + mapped ({rows:,} rows): {os.path.basename(sidecar)}")
            return column

    def _build_tags_text(frame, tag_columns, heartbeat=None):
        try:
            pool_sidecar = _mapped_source_of(frame, tag_columns) if len(frame) else None
            if pool_sidecar is not None:
                return _build_mapped(frame, tag_columns, heartbeat, _tags_sidecar_path(pool_sidecar, tag_columns))
        except Exception as exc:
            _event(f"mapped tag index failed, using the stock builder - {exc}")
        return original(frame, tag_columns, heartbeat=heartbeat)

    _build_tags_text.__doc__ = original.__doc__
    module._build_tags_text = _build_tags_text


# ── 6. 풀을 새로 앉힐 때 snapshot 두 벌을 얕은 사본으로 ─────────────────────
# 풀이 바뀌는 자리(시작 때 복원 · 새 검색)마다 업스트림은 모델에 넣은 그 프레임을 snapshot ·
# master_base 로 `.copy()` 두 번 깊은 복사한다(숫자 열 272MB x 2 / 679만 행). pandas 3 은 항상
# copy-on-write 라 얕은 사본도 서로 독립이다. 본문을 베껴 두지 않고 업스트림 소스의 그 호출만 고쳐
# 다시 컴파일한다.
def _copy_on_write() -> bool:
    try:
        import pandas as pd

        return int(str(pd.__version__).split(".", 1)[0]) >= 3
    except Exception:
        return False


def _find_function(tree, qualname: str):
    import ast

    node = tree
    for part in qualname.split("."):
        node = next(
            child for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.ClassDef)) and child.name == part
        )
    if not isinstance(node, ast.FunctionDef):
        raise ValueError(f"{qualname} is not a plain function")
    return node


def _shallow_copy_calls(function, variable: str) -> int:
    """``function``(AST) 안의 ``<variable>.copy()``(인자 없음)를 ``.copy(deep=False)`` 로 바꾸고 개수를 돌려준다."""
    import ast

    rewritten = 0
    for node in ast.walk(function):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "copy"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == variable
            and not node.args
            and not node.keywords
        ):
            node.keywords.append(ast.keyword(arg="deep", value=ast.Constant(False)))
            rewritten += 1
    return rewritten


# 재작성하는 함수에 붙어 있어도 되는 업스트림 데코레이터(같은 모듈의 이름). 정의는 `EXPECTED` 의 해시로 확인한다.
_KNOWN_DECORATORS = {"_pool_operation"}


def _function_with_shallow_copies(module, qualname: str, variable: str, expected: int):
    """``module`` 의 ``qualname``(``함수`` 또는 ``클래스.메서드``) 소스에서 ``<variable>.copy()`` 를
    얕은 사본으로 바꿔 컴파일한 함수. 호출이 정확히 ``expected`` 개가 아니면 ValueError - 모르는
    모양의 소스는 건드리지 않는다.

    줄 번호는 원본 그대로라 traceback 이 원본 파일의 그 줄을 가리킨다. 전역 이름은 모듈의 실제
    네임스페이스에서 찾는다(본문이 쓰는 ``nullcontext`` · 모듈의 다른 함수 등)."""
    import ast

    with open(module.__file__, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    function = _find_function(tree, qualname)
    # 데코레이터는 확인한 것만 받아 그대로 둔다 - 컴파일하면 업스트림처럼 그것으로 감싼 함수가 나온다.
    for decorator in function.decorator_list:
        if not (isinstance(decorator, ast.Name) and decorator.id in _KNOWN_DECORATORS):
            raise ValueError("decorated function")
        if not _guard(module, decorator.id):
            raise ValueError(f"decorator {decorator.id} changed upstream")
    rewritten = _shallow_copy_calls(function, variable)
    if rewritten != expected:
        raise ValueError(f"expected {expected} `{variable}.copy()` calls, found {rewritten}")
    code_tree = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    flags = 0
    future = getattr(module, "annotations", None)          # `from __future__ import annotations`
    if getattr(future, "compiler_flag", None):
        flags = future.compiler_flag
    namespace: dict = {}
    exec(compile(code_tree, module.__file__, "exec", flags=flags, dont_inherit=True), module.__dict__, namespace)
    built = namespace[function.name]
    built.__qualname__ = qualname
    return built


def _patch_shallow_copies(module, qualname: str, variable: str, expected: int, *, owner=None) -> None:
    """``qualname`` 을 얕은 사본판으로 바꿔 끼운다. ``owner`` 가 클래스면 메서드, 없으면 모듈 함수."""
    status_key = f"{module.__name__}:{qualname}"
    if not _copy_on_write():
        # copy-on-write 가 아니면 얕은 사본이 원본과 같이 바뀐다 - 손대지 않는다.
        STATUS["patches"][status_key] = "skipped: pandas < 3 (no copy-on-write)"
        return
    if not _guard(module, qualname):
        return
    target = owner if owner is not None else module
    name = qualname.rsplit(".", 1)[-1]
    original = target.__dict__[name] if owner is not None else getattr(module, name)
    try:
        patched = _function_with_shallow_copies(module, qualname, variable, expected)
    except Exception as exc:
        STATUS["patches"][status_key] = f"skipped: cannot rewrite ({exc})"
        return
    patched.__doc__ = original.__doc__
    ORIGINALS[name] = original
    setattr(target, name, patched)


def _patch_search_state_service(module) -> None:
    # 시작 때 복원: `frame.copy()` 두 번(snapshot · master_base).
    _patch_shallow_copies(
        module, "HeadlessSearchStateService.restore_last_search", "frame", 2,
        owner=module.HeadlessSearchStateService,
    )
    # 새 검색의 매핑 사본에 last-search parquet 의 이름을 붙인다(8 번, 아래).
    _patch_persist_last_search(module)


# ── 7. 중복 id 제거를 복사 없이 ─────────────────────────────────────────────
# 남길 행이 이보다 많은 구간으로 흩어져 있으면(= 중복이 많고 흩어져 있다) 원본 방식에 맡긴다.
MAX_DEDUP_RUNS = 1024


def _patch_dedup_by_id(module) -> None:
    if not _guard(module, "_dedup_by_id"):
        return
    np = module.np
    original = module._dedup_by_id
    ORIGINALS["_dedup_by_id"] = original

    def _dedup_by_id(frame):
        # 원본은 중복이 하나라도 있으면 `drop_duplicates(subset="id").reset_index(drop=True)` = 풀
        # 전체를 다시 만든다(arrow take: 임시본까지 풀의 2배). 아카이브 전체 검색은 820만 행 중 중복이
        # 7행인데 그 때문에 5GB 를 복사해 순간 15GB 까지 올랐다(2026-10-10 실측). 같은 행 · 같은
        # 순서를, 버릴 행 사이의 구간을 슬라이스로 이어 붙여 얻는다 - arrow 열은 버퍼를 공유한다.
        try:
            if frame is not None and not getattr(frame, "empty", True) and "id" in frame.columns:
                duplicated = frame["id"].duplicated().to_numpy()
                if not duplicated.any():
                    return frame
                keep = ~duplicated
                edges = np.flatnonzero(keep[1:] != keep[:-1]) + 1
                starts = np.concatenate(([0], edges))
                stops = np.concatenate((edges, [len(keep)]))
                spans = [(int(a), int(b)) for a, b in zip(starts, stops) if keep[a]]
                if 0 < len(spans) <= MAX_DEDUP_RUNS:
                    import pandas as pd

                    return pd.concat([frame.iloc[a:b] for a, b in spans], ignore_index=True)
        except Exception:
            pass
        return original(frame)

    _dedup_by_id.__doc__ = original.__doc__
    module._dedup_by_id = _dedup_by_id


# ── 8. 새 검색 결과를 풀에 앉히기 전에 매핑본으로 ───────────────────────────
# `run_search_command` 는 아카이브 조각을 RAM 으로 읽어 이어 붙인 프레임을 중복 제거(`_dedup_by_id`)한
# 뒤 곧바로 풀 · snapshot · master_base 로 앉힌다. 그 프레임이 그 실행 내내 RAM 에 남는다(820만 행 =
# 5.4GB). 중복 제거가 끝난 그 자리에서 프레임을 디스크 사본으로 쓰고 매핑해 **똑같은 프레임**으로
# 바꿔 넘기면, 업스트림은 처음부터 매핑된 프레임을 앉히고 RAM 프레임은 그 자리에서 버려진다. 풀에
# 앉기 전이라 snapshot 객체 identity · 풀 세대 번호에 기대는 코드(Tag Filter 캐시 · 커밋 검증)에는
# 순정과 달라지는 것이 없다.
#
# 사본은 처음엔 `<stem>-pending-<난수>.arrow` 다(parquet 이 아직 없어 키를 모른다).
# `persist_last_search` 가 last-search parquet 을 쓰면 이름을 그 parquet 의 키로 바꾼다 - 다음 시작 때
# `_read_mapped` 가 변환 없이 그대로 쓴다. 이름을 바꾸지 못한 사본(임시 검색 공간 · 기록 실패)은 이번
# 실행에서만 쓰이고 다음 정리 때 지워진다.
_SEARCH = threading.local()          # .job = 이 스레드에서 지금 도는 새 검색
_PERSIST_HOOK = {"ready": False}     # persist_last_search 를 감쌌는가(= 사본 이름을 맞춰 줄 수 있는가)


def _stem_of(sidecar: str) -> str:
    return os.path.basename(sidecar).split("-", 1)[0]


def _string_columns(table) -> list:
    import pyarrow as pa

    return [field.name for field in table.schema if pa.types.is_large_string(field.type)]


def _backs(entry, frame) -> bool:
    """``frame`` 이 매핑된 풀 ``entry`` 를 통째로(문자열 열 전부 · 같은 버퍼 · 같은 구간) 들고 있는가."""
    try:
        table = entry["table"]
        if frame is None or table.num_rows != len(frame):
            return False
        names = _string_columns(table)
        return bool(names) and all(
            name in frame.columns and _same_buffers(frame[name], table.column(name)) for name in names
        )
    except Exception:
        return False


def _frames_in(value, depth: int = 0):
    """``value``(프레임 · dict · list 가 섞인 상태 값) 안의 DataFrame 들."""
    import pandas as pd

    if isinstance(value, pd.DataFrame):
        yield value
    elif depth < 5 and isinstance(value, dict):
        for item in value.values():
            yield from _frames_in(item, depth + 1)
    elif depth < 5 and isinstance(value, (list, tuple)):
        for item in value:
            yield from _frames_in(item, depth + 1)


def _table_for_mapping(frame):
    """``frame`` 을 디스크에 썼다가 매핑해도 **똑같은 프레임**으로 되살아나는 경우에만 그 Arrow 표.

    되살아남이 보장되는 열만 받는다: numpy 숫자 · bool 열과 arrow(large_string) 문자열 열(태그
    아카이브의 열은 전부 이 둘이다). 그 밖의 열(object · 범주형 · 날짜 …)이 하나라도 있으면 None -
    그 검색은 순정대로 RAM 에 둔다."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    columns = frame.columns
    if not columns.is_unique or not all(isinstance(name, str) for name in columns):
        return None
    for name in columns:
        column = frame[name]
        if isinstance(column.dtype, np.dtype):
            if column.dtype.kind not in "iufb":
                return None
        elif isinstance(column.dtype, pd.StringDtype):
            chunked = getattr(column.array, "_pa_array", None)
            if not isinstance(chunked, pa.ChunkedArray) or not pa.types.is_large_string(chunked.type):
                return None
        else:
            return None
    table = pa.Table.from_pandas(frame, preserve_index=False)
    if table.num_rows != len(frame) or table.schema != _mappable_schema(table.schema):
        return None
    return table


def _write_table_ipc(table, path: str, batch_rows: int = 100_000) -> None:
    """Arrow 표 → 비압축 Arrow IPC 파일. 표의 버퍼를 배치 단위로 그대로 흘려 쓴다(복사하지 않는다).

    임시 이름을 거치지 않고 바로 쓴다 - ``path`` 는 한 번만 쓰이는 이름이고, 쓰다 만 파일은 부른 쪽이
    지운다."""
    import pyarrow as pa

    with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch)


def _rename(source: str, target: str, attempts: int = 5) -> None:
    """이름 바꾸기. 막 쓴 큰 파일은 백신 검사 등이 잠깐 잡고 있을 수 있어 몇 번 다시 해 본다."""
    import time

    for attempt in range(attempts):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.2)


def _map_search_frame(frame, job):
    """새 검색 결과 ``frame``(RAM)을, 디스크 사본에 얹힌 **똑같은** 프레임으로. 바꾸지 않을 때는 None.

    돌려주는 프레임은 ``frame`` 의 얕은 사본에서 문자열 열만 사본의 것으로 갈아 끼운 것이다. 숫자 열 ·
    인덱스는 ``frame`` 의 것 그대로라(사본에서 다시 읽으면 같은 값이 잠깐 두 벌이 된다) 순간 메모리가
    늘지 않는다. 사본 자체는 ``frame`` 과 Arrow 수준에서 전 열 · 전 행이 같은지 확인하고 쓴다."""
    import shutil
    import uuid

    if frame is None or getattr(frame, "empty", True):
        return None
    directory = _cache_dir()
    if not directory:
        return None
    table = _table_for_mapping(frame)
    if table is None:
        return None
    size = int(table.nbytes)
    strings = _string_columns(table)
    if size < MIN_FRAME_BYTES or not strings:
        return None
    os.makedirs(directory, exist_ok=True)
    if shutil.disk_usage(directory).free < size * 1.1 + 2 * 1024 ** 3:
        _event("new search stays in memory - not enough free disk space for a mapped copy")
        return None
    path = os.path.join(directory, f"{job['stem']}-pending-{uuid.uuid4().hex[:16]}.arrow")
    with _CONVERT_LOCK:
        if any(_backs(entry, frame) for entry in _MAPPED):
            return None                # 이미 매핑된 풀 그대로다(불러온 셋 안에서 전부 통과한 검색)
        mapped_table = strings_frame = mapped = None
        try:
            _write_table_ipc(table, path)
            mapped_table = _open_mapped(path)
            if not (mapped_table.schema.equals(table.schema, check_metadata=True) and mapped_table.equals(table)):
                raise OSError("the copy on disk differs from the search result")
            strings_frame = mapped_table.select(strings).to_pandas()
            mapped = frame.copy(deep=False)
            for name in strings:
                mapped[name] = strings_frame[name].array     # 배열로 넣는다 - 인덱스 정렬 없이 자리 그대로
            if (
                len(mapped) != len(frame)
                or not mapped.columns.equals(frame.columns)
                or list(mapped.dtypes) != list(frame.dtypes)
                or not all(_same_buffers(mapped[name], mapped_table.column(name)) for name in strings)
            ):
                raise OSError("the mapped frame does not reproduce the search result")
        except BaseException:
            mapped_table = strings_frame = mapped = None     # 매핑을 놓아야 사본을 지울 수 있다
            try:
                os.remove(path)
            except OSError:
                pass
            raise
        job["entry"] = _remember(path, mapped_table, pending=True)
    _event(f"new search mapped before install ({len(mapped):,} rows, {size / 1e6:,.0f}MB): {os.path.basename(path)}")
    return mapped


def _same_id_column(parquet_file, column) -> bool:
    """열린 parquet 의 ``id`` 열이 ``column`` 과 전 행 같은가. 행 그룹 하나씩 읽어 견준다 - 통째로 읽으면 그
    열 한 벌(820만 행 66MB)이 잠깐 더 올라온다."""
    offset = 0
    for index in range(parquet_file.metadata.num_row_groups):
        ids = parquet_file.read_row_group(index, columns=["id"]).column("id")
        if not ids.equals(column.slice(offset, len(ids))):
            return False
        offset += len(ids)
    return offset == len(column)


def _adopt_pending_copy(job, parquet) -> None:
    """방금 쓰인 last-search parquet 이 이 검색의 풀이면, 매핑 사본의 이름을 그 parquet 의 키로 바꾼다.

    parquet 이 정말 그 풀인지는 파일에서 확인한다: 행 수 · 스키마 · pandas 메타데이터 · ``id`` 열 전체.
    확인하는 동안 파일이 바뀌었으면(다른 기록이 끼어들었으면) 이름을 바꾸지 않는다."""
    import pyarrow.parquet as pq

    entry = job.get("entry")
    if entry is None or not entry["pending"]:
        return
    source = os.fspath(parquet)
    with _CONVERT_LOCK:
        if not any(known is entry for known in _MAPPED):
            return
        table = entry["table"]
        before = os.stat(source)
        if before.st_size < MIN_SOURCE_BYTES:
            return                     # 다음 시작 때 매핑하지 않는 크기다 - 사본은 이번 실행에서만 쓴다
        with pq.ParquetFile(source) as parquet_file:
            schema = parquet_file.schema_arrow
            same = (
                int(parquet_file.metadata.num_rows) == table.num_rows
                and "id" in table.column_names
                and _mappable_schema(schema).equals(table.schema, check_metadata=False)
                and (schema.metadata or {}).get(b"pandas") == (table.schema.metadata or {}).get(b"pandas")
                and _same_id_column(parquet_file, table.column("id"))
            )
        after = os.stat(source)
        final = _sidecar_path(source, before)
        if (
            not same
            or final is None
            or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
        ):
            _event("the last-search file does not hold this search - its mapped copy stays for this run only")
            return
        _rename(entry["path"], final)          # 매핑 중인 파일도 이름은 바꿀 수 있다
        entry["path"], entry["pending"] = final, False
        # 이 풀을 담은 것으로 확인한 parquet 과 그때의 키(10 번이 그 파일을 runner 로 복사해도 되는지 볼 때 쓴다).
        job["paired"] = (source, (before.st_size, before.st_mtime_ns))
        _prune(os.path.dirname(final), {known["path"] for known in _MAPPED} | _tags_sidecars_in_use())
    _event(f"mapped copy of the new search kept for the next start: {os.path.basename(final)}")


def _retire_replaced_copies(context, entry) -> None:
    """새 검색 뒤, 같은 이름의 옛 사본 가운데 다시 쓰일 일이 없는 것의 매핑을 놓고 지운다.

    다시 쓰일 일이 없는 것 = 이름을 얻지 못한 사본(pending), 그리고 이번 검색의 사본이 parquet 과
    짝지어졌다면(= last-search parquet 이 바뀌었다) 그 전 parquet 의 사본과 Tag Filter 색인. 단 지금
    살아 있는 풀(snapshot · master_base · 심층 검색 층 · 임시 검색 공간이 맡아 둔 원래 풀)이 얹혀 있는
    사본은 건드리지 않는다."""
    if not _CONVERT_LOCK.acquire(blocking=False):
        return                         # 다른 변환이 도는 중이다 - 정리는 다음 기회에
    try:
        if not any(known is entry for known in _MAPPED):
            return
        alive = list(_frames_in([
            getattr(context, "search_results_snapshot", None),
            getattr(context, "search_results_master_base_snapshot", None),
            getattr(context, "depth_state", None),
            getattr(getattr(context, "_temporary_search", None), "original", None),
        ]))
        stem = _stem_of(entry["path"])
        stale = [
            known for known in _MAPPED
            if known is not entry
            and _stem_of(known["path"]) == stem
            and (known["pending"] or not entry["pending"])
            and not any(_backs(known, frame) for frame in alive)
        ]
        if not stale:
            return
        for known in stale:
            prefix = known["path"][: -len(".arrow")] + ".tags-"
            _TAGS_IN_USE.difference_update([path for path in _TAGS_IN_USE if path.startswith(prefix)])
        _MAPPED[:] = [known for known in _MAPPED if not any(known is old for old in stale)]
        names = ", ".join(os.path.basename(known["path"]) for known in stale)
        del stale, known, alive
        _prune(os.path.dirname(entry["path"]), {known["path"] for known in _MAPPED} | _tags_sidecars_in_use())
    finally:
        _CONVERT_LOCK.release()
    _event(f"replaced pool copies released: {names}")


def _frame_to_persist(context):
    """업스트림 ``persist_last_search`` 가 last-search 파일에 쓸 프레임(그 함수가 고르는 방식 그대로)."""
    frame = getattr(context, "search_results_snapshot", None)
    if frame is None or getattr(frame, "empty", True):
        frame = getattr(context, "search_results_master_base_snapshot", None)
    return frame


def _patch_persist_last_search(module) -> None:
    qualname = "HeadlessSearchStateService.persist_last_search"
    if not _guard(module, qualname):
        return
    cls = module.HeadlessSearchStateService
    original = cls.__dict__["persist_last_search"]
    ORIGINALS["persist_last_search"] = original

    def persist_last_search(self):
        job = getattr(_SEARCH, "job", None)
        frame = _frame_to_persist(self.context) if job is not None else None
        if job is not None:
            try:
                _start_runner_draft(self.context, job, frame)      # 10 번 (다)
            except Exception as exc:
                _event(f"runner file is not written ahead - {exc}")
        path = original(self)
        if job is not None and path is not None and job["entry"] is not None and job.get("adopt", True):
            try:
                _adopt_pending_copy(job, path)
            except Exception as exc:
                _event(f"could not keep the new search's mapped copy for the next start - {exc}")
            # 방금 그 파일에 쓰인 프레임(10 번이 남은 풀과 대조한다). 파일이 이 검색의 풀로 확인됐고, 쓰는
            # 동안 그 자리의 프레임이 바뀌지 않았고, 그 프레임이 사본 그대로일 때만 적어 둔다.
            if (
                job.get("paired") is not None
                and frame is not None
                and frame is _frame_to_persist(self.context)
                and _backs(job["entry"], frame)
            ):
                job["frame"] = frame
        return path

    persist_last_search.__doc__ = original.__doc__
    cls.persist_last_search = persist_last_search
    _PERSIST_HOOK["ready"] = True


def _patch_map_new_search(module) -> bool:
    # 8 번은 6 · 7 번이 확인한 그 소스의 흐름에 기댄다: `_dedup_by_id` 가 풀에 앉히기 전 마지막 손질이고,
    # 그 프레임이 snapshot 으로 앉은 뒤 `persist_last_search` 가 불린다. 둘 중 하나라도 꺼졌으면 켜지 않는다.
    for name in ("run_search_command", "_dedup_by_id"):
        if STATUS["patches"].get(f"{module.__name__}:{name}") != "active":
            return False
    search = module.run_search_command
    dedup = module._dedup_by_id

    def _dedup_by_id(frame):
        job = getattr(_SEARCH, "job", None)
        if job is not None and frame is not None and job.get("unique") is frame:
            # 9 번이 흘려 쓰며 중복 id 를 이미 걸렀다. 순정은 중복이 없으면 받은 프레임을 그대로 돌려준다 -
            # 그것을 알아내려고 id 를 통째로 해시에 넣는 일(820만 행 +0.3GB)만 건너뛴다.
            return frame
        result = dedup(frame)
        if job is not None and job["armed"]:
            job["armed"] = False       # 검색 한 번에 한 번 - 풀에 앉힐 바로 그 프레임만
            try:
                mapped = _map_search_frame(result, job)
            except Exception as exc:
                mapped = None
                _event(f"could not map the new search, it stays in memory - {exc}")
            if mapped is not None:
                return mapped
        return result

    def run_search_command(context, command, progress_callback=None):
        if not (MAP_NEW_SEARCH and _PERSIST_HOOK["ready"]) or getattr(_SEARCH, "job", None) is not None:
            return search(context, command, progress_callback)
        # armed = 아직 매핑본으로 바꾸지 않았다 / entry = 이 검색의 사본 / adopt = 그 사본에 parquet 의
        # 이름을 붙여 다음 시작에 쓸 수 있다.
        job = {"armed": True, "entry": None, "adopt": True, "stem": "naia_last_search"}
        try:
            job["stem"] = os.path.splitext(os.path.basename(os.fspath(context.last_search_parquet_path())))[0]
        except Exception:
            pass
        _SEARCH.job = job
        try:
            return search(context, command, progress_callback)
        finally:
            _SEARCH.job = None
            if job.get("draft") is not None:
                try:
                    job["draft"].discard()         # 10 번: 쓰이지 않은 미리 쓰기를 멈추고 지운다
                except Exception:
                    pass
            if job["entry"] is not None:
                try:
                    _retire_replaced_copies(context, job["entry"])
                except Exception:
                    pass

    _dedup_by_id.__doc__ = dedup.__doc__
    run_search_command.__doc__ = search.__doc__
    module._dedup_by_id = _dedup_by_id
    module.run_search_command = run_search_command
    return True


# ── 9. 아카이브 스캔 결과를 조각 단위로 사본에 흘려 쓴다 ─────────────────────
# 8 번까지 해도 검색 **도중**의 최고치는 그대로였다(820만 행: 5.76GB). 업스트림
# `search_tag_archive_frame` 이 조각 175개의 결과를 전부 RAM 에 모은 뒤 `pd.concat` 하고, 8 번은 그
# 프레임을 통째로 사본에 옮기기 때문이다. 여기서는 그 함수를 대신해, 조각 결과를 **차례가 되는 대로**
# 8 번의 사본에 쓰고 버린다. 풀 전체가 RAM 에 올라오는 순간이 없다.
#
# 순정과 같아야 하는 것과 그 방법:
# - 행 순서 = 조각 순서. 워커가 끝나는 순서는 뒤섞이므로 차례가 된 조각만 쓴다. 앞선 조각을 기다리는
#   결과가 쌓이지 않게, 스캔 중이거나 차례를 기다리는 조각 수에 상한을 두고 그만큼만 워커에 넘긴다.
# - 숫자 열의 형. 조각마다 다르다(실제 아카이브: tokens 가 int64/double, image_width 가
#   int32/int64/double). 순정은 이어 붙이며 넓은 쪽으로 맞추는데(pd.concat), 사본의 형은 첫 줄을 쓰기
#   전에 정해야 한다. 그래서 조각 parquet 의 footer 에서 선언된 형을 모아 미리 넓혀 두고, 끝에
#   pandas 에게 "받은 형들을 이어 붙이면 무엇이 되나"를 직접 물어 대조한다. 결과가 없는 조각 때문에
#   순정 쪽이 더 좁은 형이면 숫자 열(어차피 RAM 에 있다)만 그 형으로 되돌린다 - 그 사본은 last-search
#   parquet 과 형이 달라 이번 실행에서만 쓴다. 그 밖에 어긋나면 흘려 쓴 것을 버린다.
# - 중복 id 는 **쓰기 전에** 거른다(먼저 나온 행을 남긴다 = keep-first). 다 쓴 뒤에 잘라 내면 풀이
#   "사본 그대로"가 아니게 되어(_backs · _mapped_source_of) Tag Filter 색인이 RAM 으로 간다.
# - 진행률 콜백은 순정과 같은 값으로 같은 횟수만큼 불린다.
# - 결과가 작으면(MIN_FRAME_BYTES 미만) 사본을 열지 않고 순정처럼 RAM 에서 이어 붙인다.
#
# 사본에 첫 조각들을 쓰다가 안 되는 걸 알면(디스크 부족 · 그대로 되살아난다고 보장할 수 없는 열)
# 그때까지 모은 결과를 버리지 않고 끝까지 RAM 에 모아 순정처럼 돌려준다. 그 뒤에 실패하면 사본을
# 지우고 순정 함수로 처음부터 다시 스캔한다. 어느 쪽이든 그 다음은 8 번(중복 제거 뒤 통째로 매핑)이
# 예전처럼 맡는다.
class _NotStreamable(Exception):
    """이 검색 결과는 조각 단위로 흘려 쓸 수 없다(메시지가 사유)."""


def _numeric(dtype) -> bool:
    import numpy as np

    return isinstance(dtype, np.dtype) and dtype.kind in "iuf"


def _declared_numeric_types(sources) -> dict:
    """조각 parquet 들이 선언한 숫자 열의 공통형 {열 이름: numpy dtype}. footer 만 읽는다(175개 0.04초)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    declared: dict = {}
    for item in sources:
        try:
            schema = pq.read_schema(str(item[0]))
        except Exception:
            continue                   # 읽지 못하는 조각은 스캔도 건너뛴다(search_in_file 이 None 을 낸다)
        for field in schema:
            if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
                declared.setdefault(field.name, set()).add(np.dtype(field.type.to_pandas_dtype()))
    return {name: np.result_type(*dtypes) for name, dtypes in declared.items()}


def _one_row_like(columns, dtypes):
    """그 열 · 그 형을 가진 1행짜리 프레임(값은 의미 없다)."""
    import numpy as np
    import pandas as pd

    return pd.DataFrame({
        name: pd.Series(np.zeros(1, dtype=dtype)) if isinstance(dtype, np.dtype) else pd.Series([""], dtype=dtype)
        for name, dtype in zip(columns, dtypes)
    }, columns=columns)


class _SearchSink:
    """조각 순서대로 들어오는 스캔 결과를 받아, 작을 때는 RAM 에 모으고 커지면 매핑 사본에 흘려 쓴다."""

    def __init__(self, job, sources):
        self.job = job
        self.sources = sources
        self.taken = 0                 # 지금까지 받은 조각 수(결과가 없는 조각 포함)
        self.frames: list = []         # 사본을 열기 전까지 순정처럼 쌓아 두는 결과
        self.held = 0                  # 그 크기(바이트)
        self.plain = False             # 흘려 쓰기를 포기했다 - 끝까지 RAM 에 모아 순정처럼 이어 붙인다
        self.path = self.file = self.writer = None
        self.schema = self.columns = self.dtypes = self.strings = self.seen = None
        self.kinds: set = set()        # 받은 결과들의 형 조합 - 순정이 이어 붙였을 때의 형을 묻는 데 쓴다
        self.rows = self.bytes = self.dropped = 0

    def add(self, frame) -> None:
        """차례가 된 조각의 결과 하나."""
        self.taken += 1
        if frame is None or getattr(frame, "empty", True):
            return                     # 순정도 이어 붙이기 전에 버린다
        if self.writer is not None:
            self._write(frame)
            return
        self.frames.append(frame)
        if self.plain:
            return
        self.held += int(frame.memory_usage(index=False).sum())
        if self.held < MIN_FRAME_BYTES:
            return
        try:
            self._open()
            for held in self.frames:
                self._write(held)
        except Exception as exc:
            # 아직 버린 결과가 없다 - 모아 둔 것을 들고 순정처럼 계속 간다.
            self.discard()
            self.plain = True
            _event(f"new search is not streamed to a mapped copy - {exc}")
            return
        self.frames = []

    def _open(self) -> None:
        """사본의 열 · 형을 정하고 파일을 연다. 모아 둔 결과가 MIN_FRAME_BYTES 를 넘긴 때에 불린다."""
        import shutil
        import uuid

        import numpy as np
        import pandas as pd
        import pyarrow as pa

        directory = _cache_dir()
        if not directory:
            raise _NotStreamable("no cache folder")
        columns = self.frames[0].columns
        if not columns.is_unique or not all(isinstance(name, str) for name in columns):
            raise _NotStreamable("column names that may not survive the round trip")
        declared = _declared_numeric_types(self.sources)
        dtypes = []
        for name, dtype in zip(columns, self.frames[0].dtypes):
            if _numeric(dtype):
                held = [frame.dtypes[name] for frame in self.frames[1:] if name in frame.columns]
                dtype = np.result_type(dtype, declared.get(name, dtype), *filter(_numeric, held))
            dtypes.append(dtype)
        # 빈 본보기 프레임으로 스키마를 만든다 - 다 쓴 사본을 읽은 프레임(= last-search parquet 에 쓰일
        # 프레임)의 스키마와 pandas 메타데이터까지 같아야 사본에 parquet 의 이름을 붙일 수 있다.
        template = pd.DataFrame(
            {name: pd.Series([], dtype=dtype) for name, dtype in zip(columns, dtypes)}, columns=columns
        )
        table = _table_for_mapping(template)
        if table is None or not template.columns.identical(columns) or list(template.dtypes) != dtypes:
            raise _NotStreamable("a column that may not survive the round trip")
        strings = _string_columns(table)
        if not strings:
            raise _NotStreamable("no string columns to map")
        os.makedirs(directory, exist_ok=True)
        estimate = self.held / max(1, self.taken) * len(self.sources)
        if shutil.disk_usage(directory).free < estimate * 1.2 + DISK_RESERVE_BYTES:
            raise _NotStreamable("not enough free disk space for a mapped copy")
        self.schema, self.columns, self.dtypes, self.strings = table.schema, columns, dtypes, strings
        self.seen = np.zeros(0, dtype=bool) if "id" in columns else None
        self.path = os.path.join(directory, f"{self.job['stem']}-pending-{uuid.uuid4().hex[:16]}.arrow")
        _WRITING.add(self.path)
        self.file = pa.OSFile(self.path, "wb")
        self.writer = pa.ipc.new_file(self.file, self.schema)

    def _repeated(self, ids):
        """이 조각에서 버릴 행 - 앞서 쓴 id 이거나 조각 안에서 되풀이된 id - 의 마스크. 없으면 None."""
        import numpy as np

        values = ids.to_numpy()
        if values.dtype.kind not in "iu":
            raise _NotStreamable("ids that are not integers")
        low, top = int(values.min()), int(values.max())
        if low < 0 or top >= ID_BITMAP_LIMIT:
            raise _NotStreamable("ids outside the range the duplicate check covers")
        if top >= len(self.seen):
            grown = np.zeros(min(ID_BITMAP_LIMIT, max(top + 1, 2 * len(self.seen), 1 << 22)), dtype=bool)
            grown[: len(self.seen)] = self.seen
            self.seen = grown
        drop = self.seen[values]                   # 앞선 조각에 있던 id
        drop |= ids.duplicated().to_numpy()        # 이 조각 안에서 되풀이된 id 는 첫 행만 남긴다
        self.seen[values] = True
        return drop if drop.any() else None

    def _write(self, frame) -> None:
        """결과 하나를 사본에 쓴다: 형 확인 → 중복 id 거르기 → 사본의 형으로 맞춰 쓰기."""
        import shutil

        import numpy as np
        import pyarrow as pa
        import pyarrow.compute as pc

        if not frame.columns.identical(self.columns) or frame.attrs:
            raise _NotStreamable("a shard with other columns")
        dtypes = tuple(frame.dtypes)
        for have, want in zip(dtypes, self.dtypes):
            if have != want and not (_numeric(have) and _numeric(want) and np.result_type(have, want) == want):
                raise _NotStreamable(f"a column arrives as {have} where the copy holds {want}")
        self.kinds.add(dtypes)         # 중복을 거르기 전에 - 순정은 거르기 전의 프레임들을 이어 붙인다
        if self.seen is not None:
            drop = self._repeated(frame["id"])
            if drop is not None:
                self.dropped += int(drop.sum())
                frame = frame[~drop]
                if frame.empty:
                    return
        table = _table_for_mapping(frame)
        if table is None or table.schema.names != self.schema.names:
            raise _NotStreamable("a column that may not survive the round trip")
        # 넓히는 변환뿐이고(위에서 확인), safe=True 라 값이 그대로 옮겨지지 않으면 실패한다.
        table = pa.Table.from_arrays([
            column if column.type == field.type else pc.cast(column, field.type, safe=True)
            for column, field in zip(table.columns, self.schema)
        ], schema=self.schema)
        if shutil.disk_usage(os.path.dirname(self.path)).free < table.nbytes + DISK_RESERVE_BYTES // 2:
            raise OSError("not enough free disk space for the mapped copy")
        for batch in table.to_batches(max_chunksize=100_000):
            self.writer.write_batch(batch)
        self.rows += table.num_rows
        self.bytes += table.nbytes

    def _settle_dtypes(self, frame, expected) -> bool:
        """사본에서 읽은 ``frame`` 의 숫자 열을, 순정이 이어 붙였다면 가졌을 형(``expected``)으로 맞춘다.

        보통은 이미 같다. 다른 것은 결과가 없는 조각 때문에 사본의 형이 더 넓게 잡힌 열뿐이고, 숫자 열은
        RAM 에 있으니 그 자리에서 되돌린다(넓힐 때 값이 그대로였으니 되돌려도 그대로다 - 확인한다).
        되돌린 열이 있으면 True."""
        import numpy as np

        narrowed = False
        for name, have, want in zip(list(frame.columns), list(frame.dtypes), list(expected.dtypes)):
            if have == want:
                continue
            if not (_numeric(have) and _numeric(want) and np.result_type(have, want) == have):
                raise _NotStreamable(f"stock holds {want} where the copy holds {have}")
            wide = frame[name].to_numpy()
            with np.errstate(invalid="ignore"):
                narrow = wide.astype(want)
            if not np.array_equal(narrow.astype(have), wide, equal_nan=True):
                raise _NotStreamable(f"a {have} column does not fit back into {want}")
            frame[name] = narrow
            narrowed = True
        return narrowed

    def finish(self):
        """스캔이 끝났다 → 순정이 돌려줬을 프레임(흘려 썼다면 사본에 얹힌 · 중복 id 를 이미 거른 프레임)."""
        import pandas as pd

        if self.writer is None:
            # 작은 결과 · 흘려 쓰지 못한 결과: 순정의 마무리 그대로.
            if not self.frames:
                return pd.DataFrame()
            return pd.concat(self.frames, ignore_index=True)
        # 순정이 이 결과들을 이어 붙였다면 열 · 형이 무엇이었을지 pandas 에게 묻는다.
        expected = pd.concat([_one_row_like(self.columns, kind) for kind in self.kinds], ignore_index=True)
        self.writer.close()
        self.file.close()
        self.writer = self.file = None
        with _CONVERT_LOCK:
            table = frame = None
            try:
                table = _open_mapped(self.path)
                if table.num_rows != self.rows or not table.schema.equals(self.schema, check_metadata=True):
                    raise OSError("the copy on disk is not what was written")
                frame = table.to_pandas()
                if not frame.columns.identical(expected.columns):
                    raise _NotStreamable("stock holds other columns than the copy")
                narrowed = self._settle_dtypes(frame, expected)
                if (
                    len(frame) != self.rows
                    or list(frame.dtypes) != list(expected.dtypes)
                    or not frame.index.equals(pd.RangeIndex(self.rows))
                    or not all(_same_buffers(frame[name], table.column(name)) for name in self.strings)
                ):
                    raise OSError("the mapped frame does not reproduce the search result")
            except BaseException:
                table = frame = None   # 매핑을 놓아야 사본을 지울 수 있다(discard)
                raise
            self.job["entry"] = _remember(self.path, table, pending=True)
            self.job["armed"] = False  # 이미 매핑본이다 - 중복 제거 뒤에 또 옮기지 않는다
            self.job["adopt"] = not narrowed
            # 이 프레임의 id 는 겹치지 않는다(쓰기 전에 걸렀다. id 열이 없으면 순정도 중복 제거를 하지 않는다).
            self.job["unique"] = frame
            _WRITING.discard(self.path)
            name, self.path = os.path.basename(self.path), None      # 이제 _MAPPED 가 맡는다
        _event(
            f"new search streamed to a mapped copy ({self.rows:,} rows, {self.bytes / 1e6:,.0f}MB, "
            f"{self.dropped} duplicate ids dropped{', for this run only' if narrowed else ''}): {name}"
        )
        return frame

    def discard(self) -> None:
        """쓰던 사본을 닫고 지운다. 이미 풀에 넘긴 사본(finish 가 끝난 뒤)은 건드리지 않는다."""
        for handle in (self.writer, self.file):
            try:
                if handle is not None:
                    handle.close()
            except Exception:
                pass
        self.writer = self.file = None
        path, self.path = self.path, None
        if path is None:
            return
        for attempt in range(2):
            try:
                os.remove(path)
                break
            except FileNotFoundError:
                break
            except OSError:
                if attempt == 0:
                    import gc

                    gc.collect()       # 매핑을 아직 붙들고 있는 객체가 있으면 풀어 본다
        _WRITING.discard(path)         # 못 지웠으면 다음 정리(_prune)가 지운다


def _scan_window(workers: int, query, exclude) -> int:
    """한꺼번에 스캔 중이거나 쓸 차례를 기다리게 둘 조각 수. 업스트림은 검색어 · 제외어가 있을 때 조각마다
    태그 문자열을 만든다(``search_in_file``) - 그때는 조각당 임시 메모리가 더 커서 상한을 더 낮게 잡는다."""
    limit = SCAN_WINDOW_WITH_QUERY if (query or exclude) else SCAN_WINDOW
    return max(1, min(workers, limit))


def _scan_archive(sources, query, exclude, ratings, progress_callback, sink):
    """업스트림 ``search_tag_archive_frame`` 과 같은 스캔. 다른 점은 둘이다: 결과를 끝까지 들고 있지 않고
    조각 순서대로 ``sink`` 에 넘기며, 넘기지 못한 조각이 ``_scan_window`` 를 넘지 않게 워커에 넘기는 속도를
    맞춘다(워커 스레드는 업스트림과 같은 수를 만들되 그만큼만 일을 준다). 검색 조건 · 스레드 이름 · 진행률을
    알리는 박자는 업스트림 그대로다."""
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    from core.search_engine import SearchEngine

    search_params = {
        "query": query,
        "exclude_query": exclude,
        "rating_g": "g" in ratings,
        "rating_s": "s" in ratings,
        "rating_q": "q" in ratings,
        "rating_e": "e" in ratings,
    }
    engine = SearchEngine()

    def _search_one(item):
        path, _label = item
        return engine.search_in_file(str(path), search_params)

    total = len(sources)
    max_workers = min(total, max(2, min(8, os.cpu_count() or 4)))
    step = max(1, total // 20)
    window = _scan_window(max_workers, query, exclude)
    running: dict = {}                 # future → 조각 번호
    waiting: dict = {}                 # 조각 번호 → 결과 (스캔은 끝났고 쓸 차례를 기다린다)
    submitted = handed = completed = 0
    executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="tag-archive-search")
    try:
        while handed < total:
            while submitted < total and submitted - handed < window:
                running[executor.submit(_search_one, sources[submitted])] = submitted
                submitted += 1
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                waiting[running.pop(future)] = future.result()
                completed += 1
                if progress_callback is not None and (completed % step == 0 or completed == total):
                    try:
                        progress_callback(completed, total)
                    except Exception:
                        pass
            while handed in waiting:
                sink.add(waiting.pop(handed))
                handed += 1
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    return sink.finish()


def _patch_stream_archive_scan(module) -> None:
    if not _guard(module, "search_tag_archive_frame"):
        return
    original = module.search_tag_archive_frame
    ORIGINALS["search_tag_archive_frame"] = original

    def search_tag_archive_frame(sources, *, query, exclude, ratings, progress_callback=None):
        def stock():
            return original(sources, query=query, exclude=exclude, ratings=ratings, progress_callback=progress_callback)

        # 새 검색(8 번이 표시해 둔 그 스레드)의 스캔만 흘려 쓴다. 그 밖의 호출은 순정 그대로.
        job = getattr(_SEARCH, "job", None)
        if not (STREAM_NEW_SEARCH and sources and job is not None and job["armed"] and job["entry"] is None):
            return stock()
        sink = _SearchSink(job, sources)
        try:
            return _scan_archive(sources, query, exclude, ratings, progress_callback, sink)
        except Exception as exc:
            failure = str(exc) or type(exc).__name__
        except BaseException:
            sink.discard()
            raise
        # 예외가 풀린 뒤에 지운다 - traceback 이 매핑을 붙들고 있는 동안은 Windows 가 사본을 지우지 못한다.
        sink.discard()
        _event(f"could not stream the new search to a mapped copy, scanning again the stock way - {failure}")
        return stock()

    search_tag_archive_frame.__doc__ = original.__doc__
    module.search_tag_archive_frame = search_tag_archive_frame


# ── 10. runner parquet: 풀을 이어 붙이지 않고 쓴다 · 새 검색 직후 같은 내용이면 복사한다 ──
# `save_runner_parquet` 는 풀 모델의 `get_dataframe()` 으로 남은 풀을 받아 runner parquet
# (`naia_temp_rows.parquet`)을 쓴다. 풀이 버킷 여러 개면 `get_dataframe()` 이 버킷을 `pd.concat` 해 숫자
# 열을 한 벌 더 만든다(820만 행 +0.33GB - 새 검색의 순간 최고점이 여기서 나왔다). 그리고 새 검색은
# last-search parquet 을 쓴 바로 뒤에 runner 를 또 쓴다(820만 행 각 12초).
#
# (가) 이어 붙이지 않는다. 버킷 프레임마다 Arrow 표를 만들어(복사 없음) 차례로 이으면 순정이 이어 붙인
#      프레임으로 만드는 표와 같다. 다만 parquet 의 페이지 경계는 열의 청크 경계를 따라가서, 그대로 쓰면
#      읽은 결과만 같고 바이트는 다르다(숫자 열이 순정은 한 덩어리, 여기는 버킷마다 한 청크). 그래서
#      pyarrow 가 나누는 행 그룹(1,048,576행)마다 그 구간의 숫자 열만 한 덩어리로 합쳐 넘긴다 →
#      **순정이 쓰는 파일과 바이트까지 같다.** 임시 메모리는 행 그룹 하나의 숫자 열(40MB 남짓)이다.
# (나) 새 검색 직후, 남은 풀이 방금 last-search 로 쓰인 프레임과 **행 · 순서 · 값 · 열 · 형까지** 같으면
#      인코딩하지 않고 그 파일을 복사한다(업스트림이 parquet 불러오기 뒤에 하는 것과 같은 일 -
#      core/search_pool_writer.py 의 submit). 복사본에는 last-search 의 명함(스키마 메타데이터 `naia`)이
#      딸려 온다. 순정이 쓴 runner 에는 없는 것이지만 업스트림의 그 복사 경로가 남기는 것과 같고, runner 를
#      읽는 쪽은 보지 않는다.
#      **순서가 다르면 복사하지 않는다.** 풀 모델은 행을 버킷(id 구간)별로 묶어 두는데, 아카이브 조각
#      하나(134)의 끝 5천여 행이 다른 버킷(137)의 id 를 갖고 있다. 그 조각과 137 을 함께 스캔한 검색
#      (아카이브 전체 등)은 그 행들이 137 쪽으로 묶여, 남은 풀의 행 순서가 검색 결과(= last-search)와
#      다르다(820만 행 중 18만 행의 자리). 그때는 복사할 수 없으니 (다)로 간다.
# (다) 복사할 수 없는 새 검색은 last-search 를 쓰는 동안 **다른 스레드에서 runner 를 미리 써 둔다**
#      ((가)의 방법으로, 매핑 사본 폴더의 임시 파일에). 풀은 last-search 를 쓰기 전에 이미 앉아 있고 그
#      뒤의 등급 필터 재적용은 새 검색에서는 아무것도 걸러 내지 않으므로, 그때의 풀이 곧 runner 로 쓰일
#      풀이다. runner 를 쓸 차례가 오면 지금 풀이 그때 풀과 청크를 끊은 자리까지 같은지 확인하고, 같으면
#      임시 파일을 runner 자리로 옮긴다. 순정이 차례로 두 번 인코딩하던 것(각 12초)이 겹쳐 돌 뿐, 쓰이는
#      파일은 (가)와 같다(= 순정과 같은 바이트). 다르면(그 사이 풀에서 행이 뽑혔다 등) 버리고 (가)로 쓴다.
#
# 기록은 업스트림 `SearchPoolWriter.write_now` 가 그대로 한다(세대 번호 · 대기 중인 복사 취소 · 파일 락).
# 그 안에서 불리는 `_atomic_to_parquet` 만 감싸서, 이 스레드가 프레임 대신 넘긴 표식(_RunnerPlan)일 때
# 파일을 만드는 방법만 바꾼다: runner 자리의 파일은 순정과 같은 때에, 같은 락 안에서, 임시 파일 → 교체로
# 바뀐다. 어느 방법도 안 되면 그 자리에서 순정대로(`get_dataframe()` 으로 이어 붙여) 쓴다.
ROW_GROUP_ROWS = 1024 * 1024         # pyarrow 가 parquet 을 쓸 때 기본으로 나누는 행 그룹 크기
_RUNNER = threading.local()          # .plan = 이 스레드가 지금 write_now 에 넘긴 표식
_RUNNER_HOOK = {"runtime": None}     # save_runner_parquet 를 감쌌으면 그 모듈(search_runtime)
RUNNER_COUNTS = {"copied": 0, "moved": 0, "written": 0, "stock": 0}      # 10 번이 맡은 기록의 결과(진단 · 테스트용)
# 10 번이 기대는 업스트림 함수들. 하나라도 검증한 소스가 아니면 10 번 전체를 켜지 않는다.
_RUNNER_NEEDS = (
    ("core.search_pool_writer", "_atomic_to_parquet"),                    # 감싼다(아래)
    ("core.search_pool_writer", "SearchPoolWriter.write_now"),            # 락 안에서 위 함수를 부른다
    ("core.custom_parquet_library", "write_parquet"),                     # 임시 파일 → 교체를 그대로 따른다
    ("core.search_result_model", "SearchResultModel.get_dataframe"),      # 모델에 남기는 것을 그대로 따른다
    ("core.search_result_model", "SearchResultModel._remaining_dataframe"),   # 이어 붙일 프레임 목록
)


class _RunnerPlan:
    """``write_now`` 에 프레임 대신 넘기는 표식 - 순정이 ``get_dataframe()`` 으로 이어 붙여 넘겼을 프레임 노릇."""

    def __init__(self, model, table):
        self.model = model
        self.table = table               # 버킷별 표를 이은 것 = 순정이 이어 붙인 프레임으로 만드는 표
        self.rows = table.num_rows
        self.source = self.key = None    # 대신 복사해도 되는 last-search 파일과 그때의 키 (나)
        self.draft = None                # 지금 풀 그대로 미리 써 둔 파일 (다)
        self.outcome = self.note = None  # "copied" / "moved" / "written" / "stock", 다음 방법으로 넘어간 까닭


def _pool_parts(model) -> list:
    """``model.get_dataframe()`` 이 이어 붙여 돌려줄 프레임들 - 업스트림 ``_remaining_dataframe`` 이 만드는
    그 목록이다(버킷에 남은 행을 구하는 일은 업스트림 메서드가 그대로 한다)."""
    pending = model._pending_dataframe
    if pending is not None:
        return [pending]
    buckets = model._buckets
    return [
        buckets[bucket_id].remaining_dataframe()
        for bucket_id in list(model._bucket_order)
        if bucket_id in buckets and not buckets[bucket_id].is_empty()
    ]


def _after_get_dataframe(model) -> None:
    """버킷이 둘 이상일 때 ``get_dataframe()`` 이 모델에 남기는 것 그대로(캐시 무효화 · 옛 df 포인터)."""
    import pandas as pd

    model._invalidate_caches()
    model.df = pd.DataFrame()
    model._materialized_current = False


def _parts_table(parts):
    """``parts`` 를 ``pd.concat(parts, ignore_index=True)`` 한 프레임으로 순정이 만들 Arrow 표를, 이어 붙이지
    않고 만든다. 같다고 보장할 수 없으면 None.

    열 이름 · 형 · attrs 가 조각마다 같을 때만 받는다(그러면 이어 붙여도 그대로다). 형은 numpy 숫자 · bool
    과 문자열뿐 - 조각마다 형을 따로 짐작하게 되는 열(object …)은 받지 않는다."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa

    columns, dtypes = parts[0].columns, list(parts[0].dtypes)
    if not columns.is_unique or not all(isinstance(name, str) for name in columns):
        return None
    for dtype in dtypes:
        if isinstance(dtype, np.dtype):
            if dtype.kind not in "iufb":
                return None
        elif not isinstance(dtype, pd.StringDtype):
            return None
    tables = []
    for part in parts:
        if part.attrs or not part.columns.identical(columns) or list(part.dtypes) != dtypes:
            return None
        tables.append(pa.Table.from_pandas(part, preserve_index=False))
    schema = tables[0].schema
    if not all(table.schema.equals(schema, check_metadata=True) for table in tables[1:]):
        return None
    return pa.concat_tables(tables)


def _segments(chunks, merge: bool = True) -> list:
    """Arrow 청크들이 차례로 가리키는 [버퍼들, 시작, 끝] 구간. ``merge`` 면 같은 버퍼에서 맞닿은 구간을
    하나로 잇는다(= 청크를 어디서 끊었는지는 보지 않고 내용만 본다)."""
    out: list = []
    for chunk in chunks:
        if not len(chunk):
            continue
        key = tuple((0, 0) if buffer is None else (buffer.address, buffer.size) for buffer in chunk.buffers())
        if merge and out and out[-1][0] == key and out[-1][2] == chunk.offset:
            out[-1][2] = chunk.offset + len(chunk)
        else:
            out.append([key, chunk.offset, chunk.offset + len(chunk)])
    return out


def _same_values(np, left, right, nan: bool) -> bool:
    """numpy 배열 목록 둘을 각각 차례로 이은 값이 같은가(목록마다 끊긴 자리는 달라도 된다)."""
    def place(values):
        return values.__array_interface__["data"][0], values.strides, values.dtype

    i = j = a = b = 0                  # 목록에서의 자리 · 그 배열 안에서의 자리
    while i < len(left) and j < len(right):
        count = min(len(left[i]) - a, len(right[j]) - b)
        if count:
            mine, theirs = left[i][a:a + count], right[j][b:b + count]
            # 같은 메모리의 같은 구간이면(풀의 버킷들은 한 프레임의 뷰다) 값을 읽어 볼 것도 없다.
            if place(mine) != place(theirs) and not np.array_equal(mine, theirs, equal_nan=nan):
                return False
        a, b = a + count, b + count
        if a == len(left[i]):
            i, a = i + 1, 0
        if b == len(right[j]):
            j, b = j + 1, 0
    return True


def _same_rows(left, right, same_chunks: bool = False) -> bool:
    """프레임 목록 둘을 각각 차례로 이어 붙인 것이 서로 행 · 순서 · 값 · 열 · 형까지 같은가(이어 붙여 보지 않고).

    문자열 열은 같은 버퍼의 같은 구간을 같은 순서로 들고 있어야 하고(값은 읽지 않는다 - 값만 같고 버퍼가
    다르면 같지 않다고 본다), 숫자 열은 값을 비교한다. 그 밖의 열이 있으면 False. ``same_chunks`` 면
    문자열 열의 청크를 끊은 자리까지 같아야 한다(parquet 의 페이지 경계가 거기에 달렸다)."""
    import numpy as np
    import pandas as pd

    if not left or not right or sum(len(frame) for frame in left) != sum(len(frame) for frame in right):
        return False
    columns, dtypes = left[0].columns, list(left[0].dtypes)
    if not columns.is_unique:
        return False
    for frame in (*left, *right):
        if frame.attrs or not frame.columns.identical(columns) or list(frame.dtypes) != dtypes:
            return False
    for name, dtype in zip(columns, dtypes):
        if isinstance(dtype, np.dtype) and dtype.kind in "iufb":
            sides = [[frame[name].to_numpy() for frame in side] for side in (left, right)]
            if not _same_values(np, sides[0], sides[1], dtype.kind == "f"):
                return False
        elif isinstance(dtype, pd.StringDtype):
            sides = [[getattr(frame[name].array, "_pa_array", None) for frame in side] for side in (left, right)]
            if any(array is None for side in sides for array in side):
                return False
            a, b = ([chunk for array in side for chunk in array.chunks] for side in sides)
            if _segments(a, not same_chunks) != _segments(b, not same_chunks):
                return False
        else:
            return False
    return True


def _same_row_set(parts, frame) -> bool:
    """``parts`` 가 ``frame`` 의 행들을 순서만 바꿔 묶어 놓은 것으로 보이는가 - 문자열 열마다 같은 버퍼의 같은
    구간들을 (순서와 상관없이) 빠짐없이 들고 있는가. 값은 읽지 않는다. 미리 쓰기를 시작할지 가늠하는 데만
    쓴다(써도 되는지는 나중에 ``_same_rows`` 가 정한다)."""
    import pandas as pd

    if frame is None or sum(len(part) for part in parts) != len(frame):
        return False
    names = [name for name, dtype in zip(frame.columns, frame.dtypes) if isinstance(dtype, pd.StringDtype)]
    if not names or not frame.columns.is_unique:
        return False

    def covered(frames, name):
        arrays = [getattr(each[name].array, "_pa_array", None) for each in frames]
        if any(array is None for array in arrays):
            return None
        spans: list = []
        for key, start, stop in sorted(_segments([chunk for array in arrays for chunk in array.chunks], merge=False)):
            if spans and spans[-1][0] == key and spans[-1][2] == start:
                spans[-1][2] = stop
            else:
                spans.append([key, start, stop])
        return spans

    for name in names:
        if any(name not in part.columns for part in parts):
            return False
        whole = covered([frame], name)
        if whole is None or covered(parts, name) != whole:
            return False
    return True


def _last_search_to_copy(job, parts, table):
    """이 새 검색이 방금 쓴 last-search 파일을 runner 로 복사해도 되면 (파일, 키), 아니면 None.

    ``job["paired"]`` = 이 검색의 풀을 담았다고 확인된 파일과 그때의 키(_adopt_pending_copy),
    ``job["frame"]`` = 거기에 쓰인 프레임(persist_last_search 래퍼). 남은 풀이 그 프레임과 같고, 파일의
    스키마 · pandas 메타데이터가 순정이 쓸 runner 의 것과 같아야 한다(다른 것은 명함 ``naia`` 뿐)."""
    import pyarrow.parquet as pq

    frame, paired = job.get("frame"), job.get("paired")
    if frame is None or paired is None or not _same_rows(parts, [frame]):
        return None
    schema = pq.read_schema(paired[0])
    metadata = {key: value for key, value in (schema.metadata or {}).items() if key != b"naia"}
    if metadata != dict(table.schema.metadata or {}):
        return None
    if not schema.remove_metadata().equals(table.schema.remove_metadata()):
        return None
    return paired


def _pool_to_write(runtime, context):
    """순정이 지금 runner 를 쓴다면 이어 붙일 프레임들과 그 표 → (parts, table). 10 번이 맡지 않을 때는 None:
    순정이 쓰지 않는 상태 · 이어 붙일 것이 없는 풀 · 작은 풀 · 같은 표를 보장할 수 없는 풀."""
    if runtime._should_skip_auto_runner_save(context):
        return None                    # 순정도 쓰지 않는다
    model = context.search_results
    if model is None:
        return None
    parts = _pool_parts(model)
    if len(parts) < 2 or sum(len(part) for part in parts) < MIN_RUNNER_ROWS:
        return None                    # 순정이 해도 가볍다
    table = _parts_table(parts)
    if table is None:
        return None
    return parts, table


def _plan_runner(runtime, context, job):
    """이번 runner 기록을 대신할 방법(_RunnerPlan). None 이면 순정 함수에 맡긴다."""
    pool = _pool_to_write(runtime, context)
    if pool is None:
        return None
    parts, table = pool
    plan = _RunnerPlan(context.search_results, table)
    if job is not None:
        try:
            paired = _last_search_to_copy(job, parts, table)
        except Exception as exc:
            paired, plan.note = None, f"could not compare the pool with the last-search file ({exc})"
        if paired is not None:
            plan.source, plan.key = paired
        draft = job.get("draft")
        # 미리 써 둔 파일은 그때의 풀이 지금 풀과 청크를 끊은 자리까지 같을 때만 쓴다(= 지금 쓰는 것과 같은 바이트).
        if draft is not None and paired is None and _same_rows(parts, draft.parts, same_chunks=True):
            plan.draft = draft
    _after_get_dataframe(plan.model)
    return plan


class _Abandoned(Exception):
    """미리 쓰던 runner 파일을 그만 쓰라고 했다."""


class _RunnerDraft:
    """새 검색이 last-search parquet 을 쓰는 동안 다른 스레드에서 미리 써 두는 runner parquet.

    매핑 사본 폴더의 임시 파일에 쓴다(runner 자리는 건드리지 않는다). 차례가 오면 `write_now` 의 파일 락
    안에서 runner 자리로 옮기고(``take``), 쓰이지 않으면 지운다(``discard``)."""

    def __init__(self, parts, table, path):
        self.parts = parts             # 그때의 풀 - 나중에 그때의 풀과 지금 풀이 같은지 대조한다
        self.path = path
        self.error = None
        self._table = table
        self._abandon = False
        self._thread = threading.Thread(target=self._run, name="naia-lowmem-runner-draft", daemon=True)
        _WRITING.add(path)             # 정리(_prune)가 쓰는 중인 임시 파일을 지우지 않게
        self._thread.start()

    def _run(self) -> None:
        try:
            _encode_like_stock(self._table, self.path, lambda: self._abandon)
        except BaseException as exc:
            self.error = exc
        finally:
            self._table = None

    def take(self, path) -> bool:
        """다 쓰이기를 기다렸다가 ``path`` 로 옮긴다. 옮겼으면 True."""
        from pathlib import Path

        self._thread.join()
        if self.error is not None or self._abandon or self.path is None:
            return False
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        _rename(self.path, os.fspath(path))
        _WRITING.discard(self.path)
        self.path = None
        return True

    def abandon(self) -> None:
        """그만 쓰라고 알린다(기다리지 않는다 - 쓰던 행 그룹까지만 쓰고 멈춘다)."""
        self._abandon = True

    def discard(self) -> None:
        """멈추기를 기다렸다가 임시 파일을 지운다. 이미 runner 자리로 옮겼으면 아무 일도 없다."""
        self._abandon = True
        self._thread.join()
        path, self.path = self.path, None
        if path is None:
            return
        try:
            os.remove(path)
        except OSError:
            pass                       # 못 지웠으면 다음 정리(_prune)가 지운다
        _WRITING.discard(path)


def _start_runner_draft(context, job, frame) -> None:
    """(다) 새 검색이 last-search 를 쓰기 직전: 뒤이어 쓰일 runner 를 지금부터 곁에서 써 둔다.

    ``frame`` = 곧 last-search 로 쓰일 프레임(= 검색 결과). 풀 모델에 앉아 있는 것이 그 행들이 아니면
    시작하지 않는다(아카이브 없이 불러온 셋 안에서 검색하는 갈래는 풀을 나중에 앉힌다). 순서까지 같으면
    역시 시작하지 않는다 - 그때는 다 쓰인 last-search 파일을 복사하면 된다(나)."""
    import shutil
    import uuid

    runtime = _RUNNER_HOOK.get("runtime")
    directory = _cache_dir()
    if runtime is None or not (LEAN_RUNNER and DRAFT_RUNNER) or not directory or job.get("draft") is not None:
        return
    pool = _pool_to_write(runtime, context)
    if pool is None:
        return
    parts, table = pool
    if not _same_row_set(parts, frame):
        return
    if job["entry"] is not None and job.get("adopt", True) and _same_rows(parts, [frame]):
        return
    os.makedirs(directory, exist_ok=True)
    if shutil.disk_usage(directory).free < table.nbytes + DISK_RESERVE_BYTES:
        return                         # parquet 은 이보다 훨씬 작지만 last-search 도 같이 쓰이는 중이다
    job["draft"] = _RunnerDraft(parts, table, os.path.join(directory, f"runner-draft-{uuid.uuid4().hex[:16]}.tmp"))


def _copy_as_runner(source: str, key, path) -> bool:
    """``source``(last-search parquet)가 아직 키 ``key`` 의 그 파일이면 ``path`` 로 복사한다(임시 파일 → 교체).

    파일 락 안에서 불린다 - 같은 락을 잡는 last-search 기록은 그동안 끼어들지 못한다. 그래도 복사 앞뒤로
    키를 확인해, 다른 내용이 복사됐을 수 있으면 False(= 부른 쪽이 직접 쓴다)."""
    import shutil
    from pathlib import Path

    def current():
        stat = os.stat(source)
        return stat.st_size, stat.st_mtime_ns

    if current() != key:
        return False
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")           # 업스트림의 기록 · 복사와 같은 임시 이름
    shutil.copyfile(source, tmp)
    if current() != key or os.path.getsize(tmp) != key[0]:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False
    os.replace(tmp, path)
    return True


def _encode_like_stock(table, target, abandoned=None) -> None:
    """버킷별 표를 이은 ``table`` 을 ``target`` 에, 업스트림이 이어 붙인 프레임을 ``pq.write_table`` 로 쓴
    것과 **같은 바이트**의 parquet 으로 쓴다.

    ``pq.write_table`` 은 표를 ROW_GROUP_ROWS 행씩 행 그룹으로 나눠 쓴다. 같은 일을 행 그룹마다 따로
    하되, 그 구간의 고정 폭 열(숫자 · bool)만 한 덩어리로 합쳐 넘긴다 - 순정의 표에서 그 열들은 한
    덩어리라, 그래야 페이지 경계까지 같아진다. 문자열 열은 순정의 표와 청크가 이미 같다.
    ``abandoned()`` 가 참이 되면 행 그룹 사이에서 그만둔다(_Abandoned)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        with pq.ParquetWriter(target, table.schema) as writer:
            for start in range(0, table.num_rows, ROW_GROUP_ROWS):
                if abandoned is not None and abandoned():
                    raise _Abandoned()
                group = table.slice(start, ROW_GROUP_ROWS)
                writer.write_table(pa.Table.from_arrays([
                    column.combine_chunks() if column.num_chunks > 1 and pa.types.is_primitive(column.type) else column
                    for column in group.columns
                ], schema=table.schema))
    except BaseException:
        try:
            os.remove(target)          # pq.write_table 도 쓰다 만 파일을 지우고 예외를 올린다
        except OSError:
            pass
        raise


def _write_parts_as_parquet(table, path) -> None:
    """업스트림 ``write_parquet(이어 붙인 프레임, path, None)`` 과 같은 파일을 같은 절차(임시 파일 → 교체)로."""
    from pathlib import Path

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    _encode_like_stock(table, tmp)
    os.replace(tmp, path)


def _patch_runner_parquet(module) -> None:
    key = f"{module.__name__}:save_runner_parquet"
    own = _guard(module, "save_runner_parquet")
    needs = [_guard_named(module_name, qualname) for module_name, qualname in _RUNNER_NEEDS]
    if not own:
        return
    writer_module = sys.modules.get("core.search_pool_writer")
    if not all(needs) or writer_module is None or (
        getattr(module, "search_pool_writer", None) is not getattr(writer_module, "search_pool_writer", None)
    ):
        STATUS["patches"][key] = "skipped: needs the upstream functions it writes through"
        return
    original = module.save_runner_parquet
    encode = writer_module._atomic_to_parquet
    ORIGINALS["save_runner_parquet"] = original
    ORIGINALS["_atomic_to_parquet"] = encode

    def _atomic_to_parquet(frame, path, meta=None):
        plan = getattr(_RUNNER, "plan", None)
        if plan is None or frame is not plan:
            return encode(frame, path, meta)
        # `write_now` 가 파일 락을 잡고 부른 자리다. 가벼운 방법부터: 복사(나) → 미리 써 둔 것(다) → 직접 쓰기(가).
        table, plan.table = plan.table, None
        if meta is None:
            try:
                if plan.source is not None and _copy_as_runner(plan.source, plan.key, path):
                    plan.outcome = "copied"
                    return None
            except Exception as exc:
                plan.note = f"could not copy the last-search file ({exc})"
            try:
                if plan.draft is not None and plan.draft.take(path):
                    plan.outcome = "moved"
                    return None
            except Exception as exc:
                plan.note = f"could not move the file written ahead ({exc})"
            try:
                _write_parts_as_parquet(table, path)
                plan.outcome = "written"
                return None
            except Exception as exc:
                plan.note = f"could not write the pool bucket by bucket ({exc})"
        del table
        plan.outcome = "stock"
        return encode(plan.model.get_dataframe(), path, meta)

    def save_runner_parquet(context):
        job = getattr(_SEARCH, "job", None)
        plan = None
        if LEAN_RUNNER:
            try:
                plan = _plan_runner(module, context, job)
            except Exception as exc:
                _event(f"runner file is written the stock way - {exc}")
        draft = job.get("draft") if job is not None else None
        if draft is not None and (plan is None or plan.draft is not draft):
            draft.abandon()            # 미리 쓰던 것은 쓰이지 않는다 - 그만 쓰게 한다(지우는 것은 검색이 끝날 때)
        if plan is None:
            return original(context)
        import time

        path = context.runner_parquet_path()
        started = time.perf_counter()
        _RUNNER.plan = plan
        try:
            module.search_pool_writer(context).write_now(path, plan, kind="runner")
        except Exception:
            if plan.outcome is not None:
                raise                  # 파일을 쓰다 난 예외 - 순정도 그대로 올린다
            # 표식이 기록하는 자리까지 가지 못했다 - 순정 함수가 처음부터 한다.
            _RUNNER.plan = None
            if draft is not None:
                draft.abandon()
            return original(context)
        finally:
            _RUNNER.plan = None
        RUNNER_COUNTS[plan.outcome] = RUNNER_COUNTS.get(plan.outcome, 0) + 1
        seconds = time.perf_counter() - started
        if plan.outcome == "stock":
            _event(f"runner file written the stock way - {plan.note}")
        elif job is not None:
            # 새 검색일 때만 남긴다(등급을 바꿀 때마다 남기면 events 가 이것으로 찬다).
            how = {
                "copied": "copied from the last-search file",
                "moved": "written alongside the last-search file",
                "written": "written without joining the pool",
            }[plan.outcome]
            _event(f"runner file {how} ({plan.rows:,} rows, {seconds:.1f}s)" + (f" - {plan.note}" if plan.note else ""))
        return path

    _atomic_to_parquet.__doc__ = encode.__doc__
    save_runner_parquet.__doc__ = original.__doc__
    writer_module._atomic_to_parquet = _atomic_to_parquet
    module.save_runner_parquet = save_runner_parquet
    _RUNNER_HOOK["runtime"] = module


# ── 11. 검색어가 있는 검색: 태그 문자열을 Arrow 로 이어 붙인다 ───────────────
# 업스트림은 검색어 · 제외어가 있으면 조각마다(`SearchEngine.search_in_file`) 태그 열 다섯을 쉼표로 이은
# 열(tags_string)을 `_build_tags_string` 으로 만들고, `_apply_filters` 가 그 열을 Arrow 배열로 옮겨
# (`pa.array`) PyArrow compute 로 매칭한 뒤, 살아남은 행을 고르고 그 열을 지운다. 순정의 조립은 다섯 열을
# 파이썬 문자열로 바꿔(`astype(object)`) `str.cat` 하는 것이다. 2026-10-10 실측(조각 130, 71,754행):
#   읽기 44ms · 조립 136ms · 그 결과를 Arrow 로 되옮기기 26ms · 검색어 하나 매칭 48ms · 고르기 1ms
# 조립과 되옮기기(합쳐 조각 시간의 6할)는 파이썬 쪽 일이라 워커가 몇이든 한 번에 한 스레드만 돌고(조각 24개:
# 워커 1개 5.9초 · 3개 4.5초 · 8개 4.1초), 조각마다 파이썬 문자열 43만 개를 만들었다 버려 힙을 조각낸다
# (검색 뒤에 0.2~0.4GB 가 남는다).
#
# 여기서는 `_build_tags_string` 만 대신한다. pandas 3 이 parquet 에서 읽은 문자열 열은 Arrow 배열에 얹혀
# 있으므로, 다섯 열을 `binary_join_element_wise` 로 그 자리에서 이어 붙여(12ms, 파이썬 객체 없음, GIL 을
# 놓는다) 같은 종류의 열로 돌려준다. 같은 24 조각이 워커 1개 2.1초 · 3개 1.0초 · 8개 0.6초가 된다.
#
# 순정과 같은 검색 결과인 까닭:
# - 행마다 같은 문자열이다. 순정: 값이 없는 칸(None · NaN · NA)은 ''(`na_rep=''`), 나머지는 문자열 그대로를
#   ','로 잇는다. 여기: Arrow null 은 ''(`null_replacement=''`), 나머지는 UTF-8 바이트 그대로를 ','로 잇는다.
#   Arrow 문자열 열에는 문자열과 null 밖에 없고, 올바른 UTF-8 은 파이썬 문자열로 풀었다 다시 담아도 같은
#   바이트다. 올바르지 않은 UTF-8 이 든 열은 순정이 예외를 낸다 - 그래서 잇기 전에 검사하고(`validate`),
#   걸리면 순정 함수에 넘긴다(같은 예외가 난다).
# - 그 열을 쓰는 곳은 `_apply_filters` 의 `pa.array(df['tags_string'], from_pandas=True)` 하나뿐이다. 순정의
#   object 열에서는 파이썬 문자열을 UTF-8 로 담은 Arrow 배열이, 이 열에서는 이어 붙인 그 배열이 나온다 -
#   같은 값이고, 매칭은 그 배열에만 한다. 열의 dtype 이 다른 것(object → 프레임의 다른 문자열 열과 같은
#   Arrow 문자열)은 결과에 닿지 않는다: 그 열은 행을 고른 뒤 지워진다(`search_in_file` ·
#   `filter_source_frame`).
# - 태그 열이 Arrow 문자열 배열이 아닌 프레임(object 열 · 범주형 · 숫자 열 · 형이 섞인 열), 빈 프레임,
#   같은 이름의 열이 둘인 프레임은 손대지 않고 순정 함수에 넘긴다. 예외가 나도 순정 함수로 넘어간다.
#
# `core.search_engine` 은 업스트림이 첫 검색 때 올린다(그 순서를 바꾸지 않는다) - 패치는 그때 얹힌다. 대상
# 함수들의 소스 해시는 시작 때 미리 본다(`_tags_sources_verified`): status.json 의 "active" 는 "검증한 그
# 소스다, 올라오면 얹는다"는 뜻이다.
_TAGS_KEY = "core.search_engine:SearchEngine._build_tags_string"
# 11 번이 기대는 업스트림 함수들 - 이어 붙인 열을 받아 쓰고 지우는 쪽. 하나라도 검증한 소스가 아니면 켜지 않는다.
_TAGS_NEEDS = (
    ("core.search_engine", "SearchEngine._apply_filters"),                # 그 열을 Arrow 로 옮겨 매칭한다
    ("core.search_engine", "SearchEngine.search_in_file"),                # 만들어 붙이고, 고른 뒤 지운다
    ("app.backend.server.search_runtime", "filter_source_frame"),         # `_apply_filters` 가 붙인 것을 지운다
)
TAGS_COUNTS = {"arrow": 0, "stock": 0}       # 태그 문자열을 어느 쪽이 만들었나(진단 · 테스트용)
_TAGS_LOCK = threading.Lock()                # 스캔 워커 여럿이 함께 센다
_TAGS_TOLD: set = set()                      # 순정 함수에 넘긴 사유 중 이미 events 에 남긴 것


class _NotJoinable(Exception):
    """이 프레임의 태그 열은 Arrow 로 이어 붙일 수 없다(메시지가 사유)."""


def _tags_sources_verified() -> bool:
    """11 번의 대상 함수와 그 결과를 쓰는 함수들이 전부 검증한 소스인가. 모듈을 올리지 않고 소스만 읽는다."""
    own = _guard_named("core.search_engine", "SearchEngine._build_tags_string")
    needs = [_guard_named(module_name, qualname) for module_name, qualname in _TAGS_NEEDS]
    if own and not all(needs):
        STATUS["patches"][_TAGS_KEY] = "skipped: needs the upstream functions that read the tags column"
    return own and all(needs)


def _joined_tags(frame, columns):
    """``frame`` 의 태그 열들을 쉼표로 이은 Series - 순정 ``_build_tags_string`` 과 행마다 같은 문자열,
    같은 인덱스 · 이름. 그렇다고 보장할 수 없는 프레임이면 ``_NotJoinable``(순정 함수가 맡는다)."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc

    if len(columns) < 2:
        raise _NotJoinable("fewer than two tag columns")
    first, parts = None, []
    for name in columns:
        series = frame[name]
        if not isinstance(series, pd.Series):
            raise _NotJoinable(f"two columns are named {name!r}")
        chunked = getattr(series.array, "_pa_array", None)
        if not isinstance(chunked, pa.ChunkedArray) or not (
            pa.types.is_large_string(chunked.type) or pa.types.is_string(chunked.type)
        ):
            # object · 범주형 · 숫자 … : 문자열이 아닌 값이 들었을 수 있다(순정은 그때 TypeError 를 낸다)
            raise _NotJoinable(f"column {name!r} ({series.dtype}) is not an Arrow string column")
        if parts and chunked.type != parts[0].type:
            raise _NotJoinable("tag columns of different Arrow string types")
        try:
            chunked.validate(full=True)
        except Exception:
            # 올바르지 않은 UTF-8: 순정은 파이썬 문자열로 풀다가 예외를 낸다 - 그 예외가 그대로 나게 둔다
            raise _NotJoinable(f"column {name!r} does not validate as UTF-8") from None
        parts.append(chunked)
        first = series if first is None else first
    joined = pc.binary_join_element_wise(
        *parts, pa.scalar(",", parts[0].type), null_handling="replace", null_replacement="")
    if (
        not isinstance(joined, pa.ChunkedArray)
        or joined.type != parts[0].type
        or len(joined) != len(frame)
        or joined.null_count
    ):
        raise _NotJoinable("the joined column is not one string per row")
    array = first.dtype.__from_arrow__(joined)     # 프레임의 문자열 열과 같은 종류의 배열(복사 없음)
    if type(array) is not type(first.array) or len(array) != len(frame):
        raise _NotJoinable(f"{first.dtype} does not take the joined column back as it is")
    return pd.Series(array, index=frame.index, name=first.name, copy=False)


def _patch_search_engine(module) -> None:
    if not _tags_sources_verified():
        return
    cls = module.SearchEngine
    original = cls.__dict__["_build_tags_string"]
    ORIGINALS["_build_tags_string"] = original

    def _build_tags_string(self, df):
        joined = why = None
        # 빈 프레임은 순정이 빈 object Series 를 돌려준다 - 그대로 둔다(실제 흐름에서는 여기까지 오지 않는다).
        if ARROW_TAGS and len(df):
            try:
                joined = _joined_tags(df, self.TAG_COLUMNS)
            except Exception as exc:
                why = str(exc) or type(exc).__name__
        with _TAGS_LOCK:
            TAGS_COUNTS["stock" if joined is None else "arrow"] += 1
            tell = why is not None and why not in _TAGS_TOLD and len(_TAGS_TOLD) < 8
            if tell:
                _TAGS_TOLD.add(why)
        if tell:
            _event(f"search words: tags joined the stock way - {why}")
        if joined is not None:
            return joined
        return original(self, df)

    _build_tags_string.__doc__ = original.__doc__
    cls._build_tags_string = _build_tags_string


# ── 12. 이미 앉은 풀 안에서 검색어로 좁힐 때: 구간마다 매칭한다 ──────────────
# 심층검색 [필터](`depth_search_commands.apply_depth_filters`)와 아카이브가 없을 때의 [검색](불러온 셋 안에서)은
# `filter_source_frame(풀, query=…, exclude=…)` 를 부른다. 업스트림은 풀을 깊은 복사한 뒤 `_apply_filters` 에
# 넘기고, 그 함수는 풀 **전체**의 태그 문자열을 한 번에 만들어 매칭한 다음 살아남은 행을 `df[mask]` 로 고른다.
# 그 고르기도 크다: pandas 의 take 는 Arrow 문자열 열을 고를 때 열을 통째로 하나로 이어 붙인 뒤에 고른다(600만
# 행 열 하나에 +1.2GB - 매핑된 풀이어도 그 사본은 RAM 에 생긴다). 2026-10-21 실측(아카이브 전체 검색으로 앉힌
# 820만 행, 심층검색 [열기] → [필터], 11 번까지 켠 상태 / 순정):
#   `hatsune miku`(8.6만 행) : 15.5초 · 최고 9.8GB / 순정 44.3초 · 14.6GB
#   `1girl, solo`(457만 행)  : 24.3초 · 16.8GB, 끝난 뒤에도 결과 3.1GB 가 RAM 에
#   제외어 `1girl`(257만 행) : 14.3초 · 15.1GB, 결과 2.0GB
#
# 여기서는 순정 `_apply_filters` 를 풀의 구간(2.5만 행, 여섯 구간씩 함께)마다 불러 살아남은 자리만 모은다. 같은
# 측정에서 `hatsune miku` 2.2~2.5초 · 0.66GB, `1girl, solo` 6.2~6.6초 · 0.96GB(결과는 사본에 얹혀 +0.23GB 만 남는다),
# 제외어 `1girl` 3.5~4.8초 · 0.78GB(13 번 포함 - [열기]의 +0.66GB 가 빠진 값). 순정과 같은 결과인 까닭:
# - `_apply_filters` 의 판정은 행마다 따로다(검증한 소스): 검색어마다 행별 매칭(Arrow `match_substring` ·
#   `match_substring_regex`)을 하나의 bool 배열로 AND/OR 해 나가고, 도중에 아무 행도 안 남으면 빈 프레임을
#   돌려줄 뿐이다(남은 단계는 AND 만 하므로 결과도 비게 된다). 그 문자열은 순정처럼 `_build_tags_string` 이
#   행마다 만든다(구간에 미리 만들어 넘긴다 - `_apply_filters` 는 그 열이 있으면 그것을 쓴다).
# - 그 뒤 순정이 하는 일(태그 id · 등급 조건, 마지막 복사)은 고른 행들 위에서 순정과 같은 순서 · 같은 길이로
#   한다. `df[mask]` 는 전부 True 면 고르지 않고 얕은 사본을 돌려주므로 그것도 따른다. 인덱스는 순정이 단계마다
#   고른 그대로(`Index.take` 를 같은 차례로)다 - RangeIndex 는 고른 자리가 등간격이면 RangeIndex 로 남는 등
#   한 번에 고르면 모양이 달라질 수 있다.
# - 결과가 비었을 때만 순정의 갈래(검색어 단계에서 비어 `iloc[:0]` / 제외어 단계에서 비어 `take([])`)를 구간
#   결과로 알 수 없다. 두 인덱스가 같으면(풀은 늘 0 부터의 RangeIndex 라 같다) 맡고, 다르면 순정 함수에 넘긴다.
# - 행은 Arrow 열은 `pc.filter`(청크마다 거른다 - 이어 붙이지 않는다), 나머지 열은 take 로 고른다. 결과가 크면
#   (MIN_FRAME_BYTES 이상) Arrow 문자열 열을 구간마다 디스크 사본에 흘려 쓰고 매핑해 쓴다. 사본 파일은 매핑한
#   직후 지운다 - Windows 10 1903 이후 NTFS 는 매핑 중인 파일도 지울 수 있고(이름만 사라진다) 매핑이 풀릴 때
#   공간이 돌아온다. 그래서 이 결과가 버려지면 사본도 저절로 사라진다. 지우지 못하면 다음에 다시 지워 본다.
#   새 검색(아카이브가 없을 때의 검색) 안에서도 그렇게 한다 - 그 뒤 8 번이 결과를 풀로 앉히며 사본을 한 번 더
#   쓰지만(`1girl, solo` 15.4초 → 19.5초) RAM 에 결과를 들고 있지 않아 최고치가 3.8GB → 1.28GB 다. 사본을 쓰지
#   못하면(디스크 부족 · 쓰기 실패) 결과만 RAM 에 둔다.
# 위 내용을 보장할 수 없는 프레임(이름이 겹치거나 문자열이 아닌 열 이름, 이미 tags_string 열이 있는 것)과 예외는
# 순정 함수가 맡는다(그 함수가 내는 결과 · 예외 그대로).
REFINE_COUNTS = {"pieces": 0, "mapped": 0, "stock": 0}   # 12 번이 맡은 결과 · 그중 디스크에 얹은 것 · 순정에 넘긴 것
_REFINE_LOCK = threading.Lock()
_REFINE_TOLD: set = set()                                 # 순정 함수에 넘긴 사유 중 이미 events 에 남긴 것
_REFINE_LEFTOVERS: list = []                              # 매핑한 채로 지우지 못한 결과 사본(다음에 다시 지워 본다)
_REFINE_SWEPT = {"done": False}                           # 지난 실행이 남긴 결과 사본을 치웠는가


class _NotPieceable(Exception):
    """이 프레임은 구간마다 걸러 순정과 같은 결과를 낸다고 보장할 수 없다(메시지가 사유)."""


def _refine_note(why: str, *, stock: bool = True) -> None:
    """``stock`` = 순정 함수에 넘겼다(센다) / 아니면 결과를 RAM 에 두었다는 것만 알린다. 사유는 종류마다 한 번."""
    with _REFINE_LOCK:
        if stock:
            REFINE_COUNTS["stock"] += 1
        tell = why not in _REFINE_TOLD and len(_REFINE_TOLD) < 8
        if tell:
            _REFINE_TOLD.add(why)
    if tell:
        what = "left to the stock function" if stock else "the result stays in memory"
        _event(f"narrowing a loaded pool: {what} - {why}")


def _same_index(a, b) -> bool:
    """두 인덱스가 값 · 종류 · 이름 · (RangeIndex 면) 범위까지 같은가."""
    import pandas as pd

    if not a.identical(b):
        return False
    if isinstance(a, pd.RangeIndex):
        return (a.start, a.stop, a.step) == (b.start, b.stop, b.step)
    return True


def _arrow_backed(column) -> bool:
    import pyarrow as pa

    return isinstance(getattr(column.array, "_pa_array", None), pa.ChunkedArray) and hasattr(column.dtype, "__from_arrow__")


def _picked(series, selected):
    """``series.take(selected)`` 와 같은 값 · 형의 Series. Arrow 열은 청크마다 거른다(take 는 열을 통째로 잇는다)."""
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc

    if not _arrow_backed(series):
        return series.take(selected)
    mask = np.zeros(len(series), dtype=bool)
    mask[selected] = True
    array = series.dtype.__from_arrow__(pc.filter(series.array._pa_array, pa.array(mask)))
    if type(array) is not type(series.array) or array.dtype != series.dtype or len(array) != len(selected):
        raise _NotPieceable(f"column {series.name!r} ({series.dtype}) does not come back as it was")
    return pd.Series(array, index=series.index.take(selected), name=series.name, copy=False)


def _drop_refine_files() -> None:
    """지우지 못했던 결과 사본과, 지난 실행이 남긴 결과 사본(``refine-*.arrow``)을 지워 본다."""
    if not _REFINE_SWEPT["done"]:
        _REFINE_SWEPT["done"] = True
        directory = _cache_dir()
        try:
            names = os.listdir(directory) if directory and os.path.isdir(directory) else []
        except OSError:
            names = []
        _REFINE_LEFTOVERS.extend(
            os.path.join(directory, name) for name in names
            if name.startswith("refine-") and name.endswith(".arrow"))
    for path in list(_REFINE_LEFTOVERS):
        try:
            if os.path.exists(path):
                os.remove(path)
            _REFINE_LEFTOVERS.remove(path)
        except (OSError, ValueError):
            pass


def _refined_on_disk(work, names, mask, count):
    """고른 행(``mask``)의 Arrow 문자열 열 ``names`` 를 디스크 사본에 구간마다 흘려 쓰고 매핑해 {열 이름: 배열}.
    결과가 작거나 디스크가 모자라면 None(= RAM 에서 고른다)."""
    import shutil
    import uuid

    import pyarrow as pa
    import pyarrow.compute as pc

    directory = _cache_dir()
    if not directory or not names:
        return None
    rows = len(work)
    sources = {name: work[name].array._pa_array for name in names}
    estimate = sum(source.nbytes for source in sources.values()) * count / rows
    if estimate < MIN_FRAME_BYTES:
        return None
    os.makedirs(directory, exist_ok=True)
    _drop_refine_files()
    if shutil.disk_usage(directory).free < estimate * 1.1 + DISK_RESERVE_BYTES:
        _refine_note("not enough free disk space for a mapped copy", stock=False)
        return None
    schema = pa.schema([pa.field(name, sources[name].type) for name in names])
    path = os.path.join(directory, f"refine-{uuid.uuid4().hex[:16]}.arrow")
    table = None
    _WRITING.add(path)
    try:
        with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
            for start in range(0, rows, REFINE_PIECE_ROWS):
                picks = mask[start:start + REFINE_PIECE_ROWS]
                if not picks.any():
                    continue
                picks = pa.array(picks)
                writer.write_table(pa.table(
                    {name: pc.filter(sources[name].slice(start, len(picks)), picks) for name in names}, schema=schema))
        table = _open_mapped(path)
        if table.num_rows != count or not table.schema.equals(schema):
            raise OSError("the mapped copy of the result does not hold the chosen rows")
        columns = {name: table.column(name) for name in names}
    except BaseException:
        table = columns = None              # 매핑을 놓아야 지울 수 있다
        try:
            os.remove(path)
        except OSError:
            pass
        raise
    finally:
        _WRITING.discard(path)
    del table
    try:
        os.remove(path)                     # 매핑 중에도 지워진다 - 이 결과가 버려지면 공간도 돌아온다
    except OSError:
        _REFINE_LEFTOVERS.append(path)
    return columns


def _refined_frame(work, selected, index, to_disk):
    """``work`` 의 ``selected`` 자리 행에 ``index`` 를 붙인 프레임(순정이 고른 결과와 같은 값 · 형 · 열 순서)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    arrow = [name for name in work.columns if _arrow_backed(work[name])]
    others = [name for name in work.columns if name not in set(arrow)]
    result = work[others].take(selected)        # Arrow 가 아닌 열은 이어 붙일 청크가 없다 - 그대로 고른다
    result.index = index
    if arrow:
        mask = np.zeros(len(work), dtype=bool)
        mask[selected] = True
        strings = [name for name in arrow
                   if pa.types.is_large_string(work[name].array._pa_array.type)
                   or pa.types.is_string(work[name].array._pa_array.type)]
        on_disk = None
        if to_disk:
            try:
                on_disk = _refined_on_disk(work, strings, mask, len(selected))
            except Exception as exc:
                _refine_note(f"could not map it - {exc}", stock=False)    # 사본은 지웠다 - RAM 에서 고른다
        on_disk = on_disk or {}
        picks = pa.array(mask)
        for loc, name in enumerate(work.columns):
            if name not in arrow:
                continue
            source = work[name].array
            chunked = on_disk[name] if name in on_disk else pc.filter(source._pa_array, picks)
            array = work[name].dtype.__from_arrow__(chunked)
            if type(array) is not type(source) or array.dtype != source.dtype or len(array) != len(selected):
                raise _NotPieceable(f"column {name!r} ({work[name].dtype}) does not come back as it was")
            result.insert(loc, name, array)
        if on_disk:
            with _REFINE_LOCK:
                REFINE_COUNTS["mapped"] += 1
    if list(result.columns) != list(work.columns) or list(result.dtypes) != list(work.dtypes) or len(result) != len(selected):
        raise _NotPieceable("the picked rows do not rebuild the frame")
    return result


def _filter_in_pieces(frame, query, exclude, ratings, tag_ids):
    """``filter_source_frame(frame, query=…, exclude=…, ratings=…, tag_ids=…)`` 순정과 같은 프레임을, 풀 전체의
    태그 문자열 없이. 작은 프레임이면 None(순정 함수가 맡는다), 보장할 수 없으면 ``_NotPieceable``."""
    import numpy as np
    import pandas as pd
    from concurrent.futures import ThreadPoolExecutor

    from core.search_engine import SearchEngine

    if not isinstance(frame, pd.DataFrame) or frame.empty or len(frame) < 2 * REFINE_PIECE_ROWS:
        return None
    if not frame.columns.is_unique or not all(isinstance(name, str) for name in frame.columns):
        raise _NotPieceable("column names that are not unique strings")
    if "tags_string" in frame.columns:
        raise _NotPieceable("the frame brings its own tags_string column")
    engine = SearchEngine()
    tag_columns = list(engine.TAG_COLUMNS)
    query, exclude = str(query or ""), str(exclude or "")
    rows = len(frame)
    # 순정: `filtered = frame.copy()` 에 없는 태그 열을 '' 로 채운다(copy-on-write 라 얕은 사본으로 같다).
    work = frame.copy(deep=False)
    for column in tag_columns:
        if column not in work.columns:
            work[column] = ""
    arrays = {name: work[name].array for name in tag_columns}

    def judge(start):
        stop = min(start + REFINE_PIECE_ROWS, rows)
        tagged = pd.DataFrame({name: arrays[name][start:stop] for name in tag_columns},
                              index=pd.RangeIndex(start, stop), copy=False)
        piece = pd.DataFrame({"tags_string": engine._build_tags_string(tagged)}, copy=False)
        del tagged
        positions = engine._apply_filters(piece, query, exclude).index.to_numpy()
        if len(positions) and (positions[0] < start or positions[-1] >= stop or (np.diff(positions) <= 0).any()):
            raise _NotPieceable("the stock filter did not keep rows of its piece in order")
        return positions

    hit = np.zeros(rows, dtype=bool)
    starts = range(0, rows, REFINE_PIECE_ROWS)
    workers = ThreadPoolExecutor(max_workers=max(1, min(REFINE_WORKERS, len(starts))), thread_name_prefix="lowmem-refine")
    try:
        for positions in workers.map(judge, starts):
            hit[positions] = True
    finally:
        workers.shutdown(wait=True, cancel_futures=True)       # 한 구간이 실패하면 남은 구간은 돌리지 않는다

    # 순정은 살아남은 행을 고른 프레임에 태그 id · 등급 조건을 차례로 걸고(`df[df[열].isin(값)]`) 끝에 복사한다.
    index = work.index
    if not hit.any():
        empty_early, empty_late = index[:0], index.take(np.array([], dtype=np.intp))
        if not _same_index(empty_early, empty_late):
            raise _NotPieceable("an empty result whose index depends on where stock stopped")
        selected, index = np.array([], dtype=np.intp), empty_early
    elif hit.all():
        selected = None
    else:
        selected = np.flatnonzero(hit)
        index = index.take(selected)
    del hit
    steps = []
    if tag_ids is not None and "id" in work.columns:
        steps.append(("id", tag_ids))
    if ratings and "rating" in work.columns:
        steps.append(("rating", ratings))
    for column, values in steps:
        # 순정은 고른 프레임의 그 열에 isin 을 부른다 - 같은 행 · 같은 형의 열을 만들어 같은 isin 을 부른다.
        series = work[column] if selected is None else _picked(work[column], selected)
        keep = np.asarray(series.isin(values), dtype=bool)
        del series
        if keep.all():
            continue
        chosen = np.flatnonzero(keep)
        index = index.take(chosen)
        selected = chosen if selected is None else selected[chosen]
    if selected is None:
        result = work.copy()                    # 순정의 마지막 `.copy()` - 문자열 열은 풀의 것 그대로
    else:
        result = _refined_frame(work, selected, index, MAP_REFINE)
    with _REFINE_LOCK:
        REFINE_COUNTS["pieces"] += 1
    return result


# ── 13. 심층검색을 열 때 풀 두 벌을 얕은 사본으로 ───────────────────────────
# 심층검색 [열기](와 [메인에서 새로고침])는 풀(snapshot)을 원본 · 현재 두 층으로 `base.copy()` 해 둔다. 문자열 열은
# Arrow 라 원래 버퍼를 같이 쓰지만 숫자 열이 두 벌 깊은 복사되어(820만 행 +0.66GB) 심층검색 상태가 남아 있는 내내
# RAM 에 있다. 6 번과 같은 까닭(pandas 3 은 항상 copy-on-write - 얕은 사본도 서로 독립)으로 그 네 호출만 얕은
# 사본으로 고쳐 다시 컴파일한다. 그 층들은 걸러지고(12 번 · 순정 모두 새 프레임을 만든다) 표본을 뽑고 다시 복사될
# 뿐, 제자리에서 고쳐지지 않는다(depth_search_commands · generation_commands 확인).
# 바꿔 끼우는 것은 본문 `_handle_depth_action` 이다 - 바깥의 `handle_depth_action` 은 `pool_busy` 를 잡고 그 본문을 모듈
# 전역 이름으로 부르므로 그대로 둔다.
def _patch_depth_search(module) -> None:
    _patch_shallow_copies(module, "_handle_depth_action", "base", 4)


def _patch_search_runtime(module) -> None:
    _patch_filter_source_frame(module)
    _patch_build_tags_text(module)
    _patch_dedup_by_id(module)
    # 새 검색: `searched.copy()` 세 번 - 아카이브 검색의 snapshot · master_base, 그리고 아카이브가
    # 없을 때 불러온 셋 안에서 검색하는 갈래의 snapshot.
    _patch_shallow_copies(module, "run_search_command", "searched", 3)
    if _patch_map_new_search(module):
        _patch_stream_archive_scan(module)
    else:
        # 9 번은 8 번의 흐름(새 검색 표시 · 사본 등록 · 이름 붙이기 · 옛 사본 정리)에 얹힌다.
        STATUS["patches"][f"{module.__name__}:search_tag_archive_frame"] = "skipped: needs the new-search mapping"
    _patch_runner_parquet(module)
    # 11 번의 대상(`core.search_engine`)은 첫 검색 때에야 올라온다. 검증한 소스인지는 지금 적어 둔다.
    _tags_sources_verified()


# ── import 훅 ────────────────────────────────────────────────────────────────
_PATCHERS = {
    "core.headless_search_state_service": _patch_search_state_service,
    "core.parquet_chunk_loader": _patch_parquet_chunk_loader,
    "core.search_result_model": _patch_search_result_model,
    "app.backend.server.search_runtime": _patch_search_runtime,
    "core.search_engine": _patch_search_engine,
    "app.backend.server.depth_search_commands": _patch_depth_search,
}
_PENDING = set(_PATCHERS)


class _PatchingLoader:
    """대상 모듈이 원래 로더로 다 실행된 직후 패치를 얹는다."""

    def __init__(self, loader, name):
        self._loader = loader
        self._name = name

    def create_module(self, spec):
        return self._loader.create_module(spec)

    def exec_module(self, module):
        self._loader.exec_module(module)
        try:
            _PATCHERS[self._name](module)
        except Exception as exc:
            STATUS["patches"][self._name] = f"skipped: patcher error ({exc})"
        _PENDING.discard(self._name)
        if not _PENDING:
            try:
                sys.meta_path.remove(_FINDER)
            except ValueError:
                pass
        summary = ", ".join(
            f"{key.split(':', 1)[-1]}={'on' if value == 'active' else 'OFF'}"
            for key, value in STATUS["patches"].items()
            if key.startswith(self._name)
        )
        _log(f"{self._name}: {summary or 'no patches'}")
        _write_status()

    def __getattr__(self, name):
        return getattr(self._loader, name)


class _Finder:
    def find_spec(self, fullname, path=None, target=None):
        if fullname not in _PENDING:
            return None
        from importlib.machinery import PathFinder

        spec = PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.loader is None or not hasattr(spec.loader, "exec_module"):
            return None
        spec.loader = _PatchingLoader(spec.loader, fullname)
        return spec


_FINDER = _Finder()

if ENABLED:
    sys.meta_path.insert(0, _FINDER)


def _print_fingerprints(backend_root: str) -> None:
    for key in EXPECTED:
        module_name, qualname = key.split(":", 1)
        path = os.path.join(backend_root, *module_name.split(".")) + ".py"
        try:
            found = function_fingerprint(path, qualname)
        except Exception as exc:
            found = f"ERROR {exc}"
        mark = "ok" if found in EXPECTED[key] else "CHANGED"
        print(f"{mark:8s} {found}  {key}")


if __name__ == "__main__":
    _print_fingerprints(sys.argv[1] if len(sys.argv) > 1 else os.getcwd())
