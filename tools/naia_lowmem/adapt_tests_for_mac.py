"""키트의 동등성 테스트(윈도우 설치본 기준)를 맥에서 돌릴 수 있는 사본으로 옮긴다.

    python adapt_tests_for_mac.py <키트의 lowmem 폴더> <사본을 둘 폴더>

바꾸는 것은 넷뿐이다: 설치본 경로 둘(환경변수 NAIA_TEST_BACKEND / NAIA_TEST_USER_DATA 로 받는다),
전용 메모리 측정(Win32 GetProcessMemoryInfo -> libproc 의 phys_footprint), 그리고 검색 종단 테스트가
조각을 고르는 간격(윈도우는 증분까지 175조각이라 19칸, 맥의 기본 아카이브는 150조각 - 같은 조각을
고르도록 19 를 직접 준다). 돌리는 법은 README 참고.
"""
import re
import sys

src_dir, dst_dir = sys.argv[1], sys.argv[2]

MEM = '''
import ctypes as _ct, os as _os
_libproc = _ct.CDLL("/usr/lib/libproc.dylib")
def _rusage():
    buf = (_ct.c_uint64 * 64)()
    if _libproc.proc_pid_rusage(_os.getpid(), 4, _ct.byref(buf)) != 0:
        raise OSError("proc_pid_rusage failed")
    return buf[2 + 7], buf[2 + 28]   # ri_phys_footprint, ri_lifetime_max_phys_footprint (uuid = 2 words)
'''

for name in ("test_lowmem.py", "test_lowmem_search.py"):
    with open(f"{src_dir}/{name}", encoding="utf-8") as handle:
        text = handle.read()

    def sub(pattern, repl, flags=0):
        global text
        text, count = re.subn(pattern, repl, text, count=1, flags=flags)
        assert count == 1, f"{name}: the kit changed around {pattern!r} - update this script"

    sub(r'^BACKEND = r"E:\\NAIA2\.0\\resources\\naia-backend"$', 'BACKEND = os.environ["NAIA_TEST_BACKEND"]', re.M)
    sub(r'^(REAL_)?USER_DATA = r"E:\\NAIA2\.0\\user-data"$', r'\1USER_DATA = os.environ["NAIA_TEST_USER_DATA"]', re.M)
    if name == "test_lowmem.py":
        sub(r'def private_mb\(\) -> float:\n(?:    .*\n)+?    return counters\.private / 1e6\n',
            MEM + '\ndef private_mb() -> float:\n    return _rusage()[0] / 1e6\n')
    else:
        sub(r'def memory_mb\(\) -> tuple\[float, float\]:\n(?:    .*\n)+?    return counters\.private / 1e6, counters\.peak_pagefile / 1e6\n',
            MEM + '\ndef memory_mb() -> tuple[float, float]:\n    now, top = _rusage()\n    return now / 1e6, top / 1e6\n')
        sub(r'max\(1, len\(shards\) // 9\)', '19')
    with open(f"{dst_dir}/{name}", "w", encoding="utf-8") as handle:
        handle.write(text)
    print(name, "adapted")
