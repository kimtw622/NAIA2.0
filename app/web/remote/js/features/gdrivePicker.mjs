// Google Drive 이미지 피커 (커스텀 패치, 제거 가능).
//
// 호스트 rclone 리모트를 통해 구글 드라이브 폴더를 탐색하고 이미지를 골라
// File 객체로 돌려준다. 호출자는 기존 업로드 경로(uploadModuleImage /
// resultImageInput.handleImageBlob)에 그 File을 그대로 넘기면 된다.
//
//   picker.open({title, onPick: file => ...})
//
// 백엔드: /api/gdrive/status|config|list|thumb|fetch (app/backend/server/gdrive_routes.py)
// 제거: 이 파일 + app.js wiring + imageModulePanels 'Drive' 버튼 + resultContextMenu 항목 + style.css .gdrive-*

const K_LAST_PATH = 'naia_gdrive_last_path';
const THUMB_SIZE = 192;
const THUMB_CONCURRENCY = 10;

function lsGet(key, fallback) {
  try { const value = localStorage.getItem(key); return value === null ? fallback : value; }
  catch (error) { return fallback; }
}
function lsSet(key, value) { try { localStorage.setItem(key, value); } catch (error) { /* 비치명 */ } }

function formatBytes(size) {
  const n = Number(size) || 0;
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(0)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function formatDate(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const p = v => String(v).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

export function createGdrivePicker({
  document,
  window: windowRef = (typeof window !== 'undefined' ? window : null),
  fetch: fetchFn = (typeof fetch === 'function' ? fetch.bind(globalThis) : null),
  showToast = () => {},
  escHtml = value => String(value),
} = {}) {
  const FileCtor = (windowRef && windowRef.File) || (typeof File !== 'undefined' ? File : null);
  let overlay = null;
  let ctx = null;            // {title, onPick}
  let status = null;         // /api/gdrive/status 응답
  let currentPath = '';
  let listing = null;        // 마지막 /list 응답
  let busy = false;
  let settingsOpen = false;
  let thumbObserver = null;
  let thumbQueue = [];
  let thumbActive = 0;
  let seq = 0;               // 목록 요청 레이스 방지

  function pick(selector) { return overlay ? overlay.querySelector(selector) : null; }

  function close() {
    if (thumbObserver) { try { thumbObserver.disconnect(); } catch (error) { /* noop */ } thumbObserver = null; }
    thumbQueue = [];
    thumbActive = 0;
    if (overlay) { overlay.remove(); overlay = null; }
    ctx = null;
    listing = null;
    busy = false;
    settingsOpen = false;
    if (document && document.removeEventListener) document.removeEventListener('keydown', onKeydown, true);
  }

  function onKeydown(event) {
    if (!overlay) return;
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close(); }
  }

  function setStatus(text, tone = '') {
    const el = pick('.gdrive-status');
    if (!el) return;
    el.className = 'gdrive-status' + (tone ? ' ' + tone : '');
    el.textContent = text || '';
  }

  function setBusy(flag) {
    busy = Boolean(flag);
    if (!overlay) return;
    overlay.classList.toggle('is-busy', busy);
    const grid = pick('.gdrive-grid');
    if (grid) grid.setAttribute('aria-busy', busy ? 'true' : 'false');
  }

  async function requestJson(url, init) {
    const response = await fetchFn(url, init);
    let data = {};
    try { data = await response.json(); } catch (error) { data = {}; }
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    return data;
  }

  async function loadStatus() {
    status = await requestJson('/api/gdrive/status');
    return status;
  }

  function renderShell() {
    overlay = document.createElement('div');
    overlay.className = 'gdrive-overlay';
    overlay.innerHTML = `
      <div class="gdrive-dialog" role="dialog" aria-modal="true" aria-label="Google Drive">
        <div class="gdrive-header">
          <div class="gdrive-title">
            <span class="gdrive-logo" aria-hidden="true">▲</span>
            <span class="gdrive-title-text">${escHtml((ctx && ctx.title) || 'Google Drive에서 가져오기')}</span>
          </div>
          <div class="gdrive-header-actions">
            <button type="button" class="gdrive-icon-btn gdrive-refresh" title="새로고침">↻</button>
            <button type="button" class="gdrive-icon-btn gdrive-settings-toggle" title="설정">⚙</button>
            <button type="button" class="gdrive-icon-btn gdrive-x" title="닫기 (Esc)">×</button>
          </div>
        </div>
        <div class="gdrive-settings" hidden></div>
        <div class="gdrive-crumbs"></div>
        <div class="gdrive-body">
          <div class="gdrive-grid" aria-busy="false"></div>
        </div>
        <div class="gdrive-footer">
          <span class="gdrive-status"></span>
          <span class="gdrive-hint">폴더를 열고 사진을 클릭하면 바로 업로드됩니다 · Esc 닫기</span>
        </div>
      </div>`;
    overlay.addEventListener('mousedown', event => { if (event.target === overlay) close(); });
    pick('.gdrive-x').addEventListener('click', close);
    pick('.gdrive-refresh').addEventListener('click', () => { if (!busy) loadDir(currentPath, {force: true}); });
    pick('.gdrive-settings-toggle').addEventListener('click', () => toggleSettings());
    document.body.appendChild(overlay);
    document.addEventListener('keydown', onKeydown, true);
  }

  function renderSettings() {
    const box = pick('.gdrive-settings');
    if (!box) return;
    box.hidden = !settingsOpen;
    if (!settingsOpen) return;
    const st = status || {};
    const remotes = Array.isArray(st.remotes) ? st.remotes : [];
    const remoteOptions = remotes.length
      ? remotes.map(r => `<option value="${escHtml(r)}" ${r === st.remote ? 'selected' : ''}>${escHtml(r)}</option>`).join('')
      : `<option value="${escHtml(st.remote || '')}" selected>${escHtml(st.remote || '(없음)')}</option>`;
    box.innerHTML = `
      <div class="gdrive-settings-row">
        <label>rclone 리모트
          <select class="gdrive-remote-select" ${st.remote_locked ? 'disabled title="NAIA_GDRIVE_REMOTE 환경변수로 고정됨"' : ''}>${remoteOptions}</select>
        </label>
        <label>시작 폴더 (리모트 기준 경로, 비우면 드라이브 루트)
          <input type="text" class="gdrive-root-input" value="${escHtml(st.root || '')}" placeholder="예: Novel AI/refs" ${st.root_locked ? 'disabled title="NAIA_GDRIVE_ROOT 환경변수로 고정됨"' : ''}>
        </label>
        <button type="button" class="gdrive-btn gdrive-settings-save">저장</button>
      </div>
      <div class="gdrive-settings-meta">
        rclone: <code>${escHtml(st.rclone_bin || '(없음)')}</code>
        ${st.error ? `<span class="gdrive-settings-error">${escHtml(st.error)}</span>` : ''}
      </div>
      <div class="gdrive-settings-help">
        서버 PC에 <code>rclone</code>이 설치돼 있고 <code>rclone config</code>로 Google Drive 리모트를 만들어 두어야 합니다.
        다른 PC에서 쓰려면 <code>rclone.conf</code>를 복사하면 됩니다.
      </div>`;
    pick('.gdrive-settings-save').addEventListener('click', async () => {
      const remote = pick('.gdrive-remote-select');
      const root = pick('.gdrive-root-input');
      const payload = {};
      if (remote && !remote.disabled) payload.remote = remote.value;
      if (root && !root.disabled) payload.root = root.value;
      try {
        setBusy(true);
        status = await requestJson('/api/gdrive/config', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload),
        });
        showToast('Google Drive 설정을 저장했습니다', 'success');
        settingsOpen = false;
        renderSettings();
        currentPath = '';
        lsSet(K_LAST_PATH, '');
        await loadDir('', {force: true});
      } catch (error) {
        showToast(error.message || '설정 저장 실패', 'error');
        setBusy(false);
      }
    });
  }

  function toggleSettings(force) {
    settingsOpen = typeof force === 'boolean' ? force : !settingsOpen;
    renderSettings();
  }

  function renderCrumbs(data) {
    const el = pick('.gdrive-crumbs');
    if (!el) return;
    const rootLabel = data && data.root ? `${escHtml(data.remote || 'gdrive')}:${escHtml(data.root)}` : `${escHtml((data && data.remote) || 'gdrive')}:`;
    const crumbs = (data && Array.isArray(data.crumbs)) ? data.crumbs : [];
    el.innerHTML = `
      <button type="button" class="gdrive-crumb gdrive-crumb-root" data-path="" title="${rootLabel}">${crumbs.length ? '내 드라이브' : '<b>내 드라이브</b>'}</button>
      ${crumbs.map((c, i) => `
        <span class="gdrive-crumb-sep">›</span>
        <button type="button" class="gdrive-crumb" data-path="${escHtml(c.path)}">${i === crumbs.length - 1 ? `<b>${escHtml(c.name)}</b>` : escHtml(c.name)}</button>
      `).join('')}`;
    el.querySelectorAll('.gdrive-crumb').forEach(btn => {
      btn.addEventListener('click', () => { if (!busy) loadDir(btn.dataset.path || ''); });
    });
  }

  // 서버 생성 썸네일(폴백). 보통은 Drive가 준 thumbnailLink(item.thumb)를 브라우저가 직접 받는다.
  function serverThumbUrl(item) {
    const q = new URLSearchParams({path: item.path, id: item.id || '', size: String(THUMB_SIZE), mtime: item.mod_time || ''});
    return `/api/gdrive/thumb?${q.toString()}`;
  }

  function pumpThumbs() {
    while (thumbActive < THUMB_CONCURRENCY && thumbQueue.length) {
      const img = thumbQueue.shift();
      if (!img || !img.isConnected || img.dataset.loaded === '1') continue;
      thumbActive += 1;
      const done = () => { thumbActive = Math.max(0, thumbActive - 1); pumpThumbs(); };
      const onLoad = () => { img.dataset.loaded = '1'; img.classList.add('is-loaded'); done(); };
      const onError = () => {
        const fallback = img.dataset.fallback;
        if (fallback && img.src !== fallback && !img.dataset.fellBack) {
          img.dataset.fellBack = '1';
          img.src = fallback;   // Drive 썸네일 링크 만료/차단 → 서버 썸네일로 재시도
          return;
        }
        img.removeEventListener('load', onLoad);
        img.classList.add('is-error');
        done();
      };
      img.addEventListener('load', onLoad, {once: true});
      img.addEventListener('error', onError);
      img.src = img.dataset.src;
    }
  }

  function enqueueThumb(img) {
    if (!img || img.dataset.queued === '1') return;
    img.dataset.queued = '1';
    thumbQueue.push(img);
    pumpThumbs();
  }

  function ensureObserver() {
    if (thumbObserver || !windowRef || typeof windowRef.IntersectionObserver !== 'function') return;
    const root = pick('.gdrive-body');
    thumbObserver = new windowRef.IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          enqueueThumb(entry.target);
          thumbObserver.unobserve(entry.target);
        }
      });
    }, {root, rootMargin: '200px'});
  }

  function renderGrid(data) {
    const grid = pick('.gdrive-grid');
    if (!grid) return;
    thumbQueue = [];
    if (thumbObserver) { try { thumbObserver.disconnect(); } catch (error) { /* noop */ } thumbObserver = null; }
    const folders = (data && data.folders) || [];
    const images = (data && data.images) || [];
    if (!folders.length && !images.length) {
      grid.innerHTML = '<div class="gdrive-empty">이 폴더에 하위 폴더나 이미지가 없습니다</div>';
      return;
    }
    grid.innerHTML = `
      ${folders.map(f => `
        <button type="button" class="gdrive-item gdrive-folder" data-path="${escHtml(f.path)}" data-id="${escHtml(f.id || '')}" title="${escHtml(f.name)}">
          <span class="gdrive-folder-icon" aria-hidden="true">📁</span>
          <span class="gdrive-item-name">${escHtml(f.name)}</span>
        </button>`).join('')}
      ${images.map(img => `
        <button type="button" class="gdrive-item gdrive-image" data-path="${escHtml(img.path)}" data-id="${escHtml(img.id || '')}" data-name="${escHtml(img.name)}" title="${escHtml(img.name)}&#10;${escHtml(formatDate(img.mod_time))} · ${escHtml(formatBytes(img.size))}">
          <span class="gdrive-thumb-wrap"><img class="gdrive-thumb" alt="" data-src="${escHtml(img.thumb || serverThumbUrl(img))}" data-fallback="${escHtml(serverThumbUrl(img))}" referrerpolicy="no-referrer" loading="lazy"></span>
          <span class="gdrive-item-name">${escHtml(img.name)}</span>
          <span class="gdrive-item-meta">${escHtml(formatBytes(img.size))}</span>
        </button>`).join('')}`;
    grid.querySelectorAll('.gdrive-folder').forEach(btn => {
      btn.addEventListener('click', () => { if (!busy) loadDir(btn.dataset.path || '', {id: btn.dataset.id || ''}); });
    });
    grid.querySelectorAll('.gdrive-image').forEach(btn => {
      btn.addEventListener('click', () => pickImage(btn.dataset.path || '', btn.dataset.name || '', btn.dataset.id || ''));
    });
    ensureObserver();
    grid.querySelectorAll('img.gdrive-thumb').forEach(img => {
      if (thumbObserver) thumbObserver.observe(img);
      else enqueueThumb(img);
    });
  }

  async function loadDir(path, {force = false, id = ''} = {}) {
    if (!overlay) return;
    const mySeq = ++seq;
    setBusy(true);
    setStatus('불러오는 중…');
    try {
      const q = new URLSearchParams({path: path || ''});
      if (id) q.set('id', id);
      if (force) { q.set('refresh', '1'); q.set('_', String(Date.now())); }
      const data = await requestJson(`/api/gdrive/list?${q.toString()}`);
      if (mySeq !== seq || !overlay) return;
      listing = data;
      currentPath = data.path || '';
      lsSet(K_LAST_PATH, currentPath);
      renderCrumbs(data);
      renderGrid(data);
      const n = (data.images || []).length;
      const f = (data.folders || []).length;
      setStatus(`${f ? `폴더 ${f}개 · ` : ''}이미지 ${n}개${data.api === 'rclone' ? ' · rclone 경로(느림)' : ''}`);
      if (data.note) console.warn('Google Drive picker: Drive API 폴백 →', data.note);
      const body = pick('.gdrive-body');
      if (body) body.scrollTop = 0;
    } catch (error) {
      if (mySeq !== seq || !overlay) return;
      console.error('Google Drive list failed', error);
      setStatus(error.message || '목록을 불러오지 못했습니다', 'error');
      // 저장된 마지막 경로가 사라졌을 수 있다 — 루트로 한 번 폴백.
      if (path && /not found|찾을 수|404/i.test(String(error.message || ''))) {
        lsSet(K_LAST_PATH, '');
        await loadDir('', {force});
        return;
      }
      const grid = pick('.gdrive-grid');
      if (grid) grid.innerHTML = `<div class="gdrive-empty gdrive-empty-error">${escHtml(error.message || '오류')}</div>`;
      if (status && !status.available) toggleSettings(true);
    } finally {
      if (mySeq === seq) setBusy(false);
    }
  }

  async function pickImage(path, name, id = '') {
    if (!path || busy || !ctx) return;
    setBusy(true);
    setStatus(`${name || path} 가져오는 중… (원본 크기에 따라 수 초~수십 초)`);
    try {
      const response = await fetchFn('/api/gdrive/fetch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({path, id}),
      });
      if (!response.ok) {
        let detail = `HTTP ${response.status}`;
        try { const data = await response.json(); if (data && data.error) detail = data.error; } catch (error) { /* noop */ }
        throw new Error(detail);
      }
      const blob = await response.blob();
      let filename = name || path.split('/').pop() || 'gdrive-image';
      const headerName = response.headers.get('X-NAIA-Filename');
      if (headerName) { try { filename = decodeURIComponent(headerName); } catch (error) { /* noop */ } }
      const type = blob.type && blob.type.startsWith('image/') ? blob.type : guessType(filename);
      const file = FileCtor ? new FileCtor([blob], filename, {type}) : blob;
      const onPick = ctx.onPick;
      close();
      if (typeof onPick === 'function') await onPick(file, {path, name: filename});
    } catch (error) {
      console.error('Google Drive fetch failed', error);
      showToast(error.message || 'Google Drive에서 이미지를 가져오지 못했습니다', 'error');
      setStatus(error.message || '가져오기 실패', 'error');
      setBusy(false);
    }
  }

  function guessType(filename) {
    const ext = String(filename || '').toLowerCase().split('.').pop();
    return ({png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', webp: 'image/webp', gif: 'image/gif', bmp: 'image/bmp', avif: 'image/avif'})[ext] || 'image/png';
  }

  async function open(options = {}) {
    if (!fetchFn || !document) return;
    if (overlay) close();
    ctx = {title: options.title || 'Google Drive에서 가져오기', onPick: options.onPick};
    renderShell();
    setBusy(true);
    setStatus('Google Drive 연결 확인 중…');
    try {
      await loadStatus();
    } catch (error) {
      status = {available: false, error: error.message || String(error), remotes: []};
    }
    if (!overlay) return;
    if (!status || !status.available) {
      setBusy(false);
      setStatus(status && status.error ? status.error : 'Google Drive를 사용할 수 없습니다', 'error');
      const grid = pick('.gdrive-grid');
      if (grid) grid.innerHTML = `<div class="gdrive-empty gdrive-empty-error">${escHtml((status && status.error) || 'rclone 설정을 확인하세요')}</div>`;
      toggleSettings(true);
      return;
    }
    const start = options.path !== undefined ? String(options.path || '') : lsGet(K_LAST_PATH, '');
    await loadDir(start);
  }

  function isOpen() { return Boolean(overlay); }

  return {open, close, isOpen};
}
