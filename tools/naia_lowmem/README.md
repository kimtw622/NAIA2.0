# naia_lowmem (커스텀, 제거 가능)

백엔드가 검색 풀을 RAM 에 여러 벌 올리지 않게 하는 애드온. 원본과 설명 · 실측 · 동등성 테스트
(`test_lowmem.py`, `test_lowmem_search.py` - 윈도우 설치본 경로 기준)는
[kimtw622/naia-custom-patches](https://github.com/kimtw622/naia-custom-patches) 의 `lowmem/` 에 있다.
여기 두 파일은 그 사본이다(가져온 커밋 `7cebf38`, 모듈 `__version__` "6").

- 설치: `tools/naia_lowmem/install.sh` — `naia_lowmem.py` + `naia_lowmem.pth` 를 `venv` 의
  site-packages 로 복사하고 대상 함수의 소스 해시를 찍는다(전부 `ok` 여야 한다).
  venv 는 git 에 없으므로 **venv 를 새로 만들면 다시 돌린다.**
- 끄기: `tools/naia_lowmem/install.sh --off` (또는 환경변수 `NAIA_LOWMEM=0`) 후 NAIA 재시작.
- 상태: `<user-data>/cache/lowmem/status.json` 의 `patches` 가 전부 `active` 인지.
  맥의 user-data 는 `~/Library/Application Support/NAIA`.
- NAIA 를 올린 뒤 해시가 `CHANGED` 로 찍히면 그 패치만 꺼진다(순정 동작). 갱신은 키트 저장소에서
  테스트를 통과시킨 뒤 `EXPECTED` 를 고치고 두 파일을 다시 가져온다.
