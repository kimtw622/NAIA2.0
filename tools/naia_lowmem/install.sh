#!/bin/bash
# naia_lowmem 을 이 체크아웃의 venv 에 넣는다(.pth 가 인터프리터 시작 때 모듈을 올린다).
# venv 를 새로 만들었거나 naia_lowmem.py 를 갱신했을 때 다시 돌린다. 끄기: --off (.pth 만 지운다).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
py="$root/venv/bin/python"
[ -x "$py" ] || { echo "venv not found: $py" >&2; exit 1; }
sp="$("$py" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
if [ "${1:-}" = "--off" ]; then
  rm -f "$sp/naia_lowmem.pth"
  echo "naia_lowmem off (removed $sp/naia_lowmem.pth)"
  exit 0
fi
cp "$here/naia_lowmem.py" "$here/naia_lowmem.pth" "$sp/"
echo "naia_lowmem installed into $sp"
"$py" -B "$here/naia_lowmem.py" "$root"
