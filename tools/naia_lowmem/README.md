# naia_lowmem (커스텀, 제거 가능)

백엔드가 검색 풀을 RAM 에 여러 벌 올리지 않게 하는 애드온. 원본과 설명 · 실측 · 동등성 테스트
(`test_lowmem.py`, `test_lowmem_search.py` - 윈도우 설치본 경로 기준)는
[kimtw622/naia-custom-patches](https://github.com/kimtw622/naia-custom-patches) 의 `lowmem/` 에 있다.
여기 두 파일은 그 사본이다(키트의 v2.0.65 이식 때 것, 모듈 `__version__` "7". `.pth` 는 저장소의 `*.pth` 무시 규칙에 걸려 `git add -f` 로 넣었다).

- 설치: `tools/naia_lowmem/install.sh` — `naia_lowmem.py` + `naia_lowmem.pth` 를 `venv` 의
  site-packages 로 복사하고 대상 함수의 소스 해시를 찍는다(전부 `ok` 여야 한다).
  venv 는 git 에 없으므로 **venv 를 새로 만들면 다시 돌린다.**
- 끄기: `tools/naia_lowmem/install.sh --off` (또는 환경변수 `NAIA_LOWMEM=0`) 후 NAIA 재시작.
- 상태: `<user-data>/cache/lowmem/status.json` 의 `patches` 가 전부 `active` 인지.
  맥의 user-data 는 `~/Library/Application Support/NAIA`.
- NAIA 를 올린 뒤 해시가 `CHANGED` 로 찍히면 그 패치만 꺼진다(순정 동작). 갱신은 키트 저장소에서
  테스트를 통과시킨 뒤 `EXPECTED` 를 고치고 두 파일을 다시 가져온다.

## 맥에서 동등성 테스트 돌리기

키트의 테스트는 윈도우 설치본 경로와 Win32 메모리 측정을 쓴다. `adapt_tests_for_mac.py` 가 그 네
군데만 바꾼 사본을 만든다. **실제 user-data 는 쓰지 않는다** — 아카이브(`data/tags`)를 복제한 임시
user-data 를 주고, NAIA 는 꺼 두거나 그대로 둬도 된다(임시 폴더에서만 읽고 쓴다, 메모리 3GB 쯤).

```bash
KIT=<naia-custom-patches 체크아웃>/lowmem      # 테스트 원본
T=$(mktemp -d)                                  # 작업 폴더
mkdir -p "$T/tests" "$T/ud/data"
cp -Rc "$HOME/Library/Application Support/NAIA/data/tags" "$T/ud/data/tags"   # APFS 복제(공간 안 씀)
./venv/bin/python -I tools/naia_lowmem/adapt_tests_for_mac.py "$KIT" "$T/tests"
cp "$KIT/naia_lowmem.py" "$T/tests/"            # 테스트 대상은 테스트 옆의 사본이다
# test_lowmem.py 는 실제 풀의 가운데 행 그룹을 샘플로 쓴다: $T/ud/cache/naia_last_search.parquet 가
# 있어야 한다(임시 user-data 로 NAIA 를 띄워 아카이브 전체를 한 번 검색하면 생긴다).
cd "$T/tests" && NAIA_TEST_BACKEND=<이 체크아웃> NAIA_TEST_USER_DATA="$T/ud" \
  <이 체크아웃>/venv/bin/python -B test_lowmem.py "$T/lowmem-test"
```

2026-10-11, v2.0.54 · 맥(pandas 3.0.5, pyarrow 25.0.1, 아카이브 150조각): 123 + 366 항목 통과.
아카이브 전체 새 검색(679만 행)은 14초, 프로세스 최고 1.2GB · 검색 뒤 0.5GB(phys_footprint).
