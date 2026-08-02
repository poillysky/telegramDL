const state = {
  selectedChats: [],
  chats: [],
  pollTimer: null,
  account: null,
  tgAuthorized: false,
  tgStep: "api",
  webUsername: null,
  webUsers: [],
  historyOffset: 0,
  historyLimit: 30,
  historyTotal: 0,
  historyTimer: null,
  page: "tasks",
  indexMeta: null,
  indexCoverage: null,
  indexTags: [],
  indexPolling: false,
  indexTimer: null,
  indexPreviewTotal: null,
  monitorTags: [],
  monitorTagMeta: {}, // tagLower -> { related?: bool, seed?: bool }
  tagRelated: {}, // tag -> [related...]
  editingTagIndex: -1,
  tagSearch: "",
  tagSuggestItems: [],
  tagSuggestIndex: -1,
  tasks: [],
  taskTagsDraft: null, // { taskId, chatId, tags, groups: string[][], ... }
  tagPickerIndex: [], // [{tag, count}]
  tagPickerBundles: [], // [{tags: string[], count}] same-caption groups
  tagPickerRelated: {}, // tag -> [related...]
  tagPickerPage: "index", // mobile tabs: applied | index
  _tagPickerSearchTimer: null,
  _tasksInflight: null,
  _tasksQueued: false,
  _chatsInflight: null,
  _historyInflight: null,
  _chatSearchTimer: null,
  _indexTagsSig: "",
  _indexWasScanning: false,
  tagBlacklist: [], // display casing from server
  tagBlacklistDefaults: [],
};

const STATUS_LABELS = {
  pending: "等待中",
  running: "下载中",
  paused: "已暂停",
  completed: "已完成",
  failed: "失败",
};

function statusLabel(status, task) {
  if (
    status === "running" &&
    task &&
    normalizeDownloadMode(task.download_mode) === "monitor"
  ) {
    return "监控中";
  }
  return STATUS_LABELS[status] || status || "未知";
}

class SoftAuthError extends Error {
  constructor(message) {
    super(message || "需要 Web 登录");
    this.name = "SoftAuthError";
    this.softAuth = true;
  }
}

function isSoftAuthError(e) {
  return !!(e && (e.softAuth || e.name === "SoftAuthError"));
}

async function confirmWebSessionLost() {
  try {
    const web = await Promise.race([
      fetch("/api/auth/web-session", { credentials: "same-origin" }).then((r) =>
        r.json()
      ),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("timeout")), 3000)
      ),
    ]);
    if (!web) return false;
    if (!web.need_password) return false;
    return !web.authenticated;
  } catch (_) {
    // Network blip — do NOT treat as logout (esp. iOS background resume)
    return false;
  }
}

async function api(path, options = {}) {
  const { headers: extraHeaders, signal, ...rest } = options;
  const opts = {
    credentials: "same-origin",
    ...rest,
    headers: { "Content-Type": "application/json", ...(extraHeaders || {}) },
  };
  if (signal) opts.signal = signal;
  const res = await fetch(path, opts);
  if (res.status === 401) {
    const p = String(path);
    const authProbe =
      p.includes("/web-login") ||
      p.includes("/web-status") ||
      p.includes("/web-session");
    if (!authProbe) {
      // Avoid kicking to login on a single 401 (iOS wake / proxy blip)
      if (!state._authKickInflight) {
        state._authKickInflight = confirmWebSessionLost()
          .then((lost) => {
            if (lost) {
              clearWebAuthedHint();
              showWebLogin();
            }
          })
          .finally(() => {
            state._authKickInflight = null;
          });
      }
    }
    const data = await res.json().catch(() => ({}));
    throw new SoftAuthError(data.detail || data.message || "需要 Web 登录");
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.detail || data.message || res.statusText);
  }
  return data;
}

function $(id) {
  return document.getElementById(id);
}

function normalizeMsgType(text, type) {
  const t = String(text || "").trim();
  if (type === "ok" || type === "err" || type === "info") return type;
  if (/中…$|中\.\.\.$|保存中|创建中|发送中|登录中|测试中|计算中/.test(t)) return "info";
  return type || "info";
}

function setMsg(el, text, type = "") {
  if (!el) return;
  const t = String(text || "").trim();
  if (!t) {
    el.hidden = true;
    el.className = "msg";
    el.innerHTML = "";
    return;
  }
  const kind = normalizeMsgType(t, type);
  const loading = kind === "info" && /中…$|中\.\.\.$|保存中|创建中|发送中|登录中|测试中/.test(t);
  el.hidden = false;
  el.className = `msg msg-${kind} is-visible${loading ? " is-loading" : ""}`;
  // keep legacy aliases used elsewhere
  if (kind === "ok") el.classList.add("ok");
  if (kind === "err") el.classList.add("err");
  if (kind === "info") el.classList.add("info");
  el.innerHTML = `<span class="msg-icon" aria-hidden="true"></span><span class="msg-text"></span>`;
  el.querySelector(".msg-text").textContent = t;
}

let _confirmResolver = null;
let _confirmKeyHandler = null;

function closeConfirmDialog(result) {
  const modal = $("confirmModal");
  if (!modal || modal.hidden) {
    if (_confirmResolver) {
      const resolve = _confirmResolver;
      _confirmResolver = null;
      resolve(!!result);
    }
    return;
  }
  modal.hidden = true;
  document.body.classList.remove("confirm-open");
  if (_confirmKeyHandler) {
    document.removeEventListener("keydown", _confirmKeyHandler);
    _confirmKeyHandler = null;
  }
  const resolve = _confirmResolver;
  _confirmResolver = null;
  if (resolve) resolve(!!result);
}

/**
 * Custom confirm (replaces window.confirm).
 * @returns {Promise<boolean>}
 */
function confirmDialog(opts = {}) {
  const modal = $("confirmModal");
  if (!modal) return Promise.resolve(false);
  // Close any prior dialog as cancel
  if (_confirmResolver) closeConfirmDialog(false);

  const title = opts.title || "请确认";
  const message = opts.message || "";
  const confirmText = opts.confirmText || "确定";
  const cancelText = opts.cancelText || "取消";
  const danger = !!opts.danger;
  const alertOnly = !!opts.alertOnly;

  const panel = modal.querySelector(".confirm-panel");
  const kicker = $("confirmKicker");
  const titleEl = $("confirmTitle");
  const msgEl = $("confirmMessage");
  const btnOk = $("btnConfirmOk");
  const btnCancel = $("btnConfirmCancel");

  if (panel) panel.classList.toggle("is-danger", danger);
  if (kicker) kicker.textContent = alertOnly ? "提示" : danger ? "危险操作" : "确认";
  if (titleEl) titleEl.textContent = title;
  if (msgEl) msgEl.textContent = message;
  if (btnOk) {
    btnOk.textContent = confirmText;
    btnOk.className = danger ? "danger" : "primary";
  }
  if (btnCancel) {
    btnCancel.textContent = cancelText;
    btnCancel.hidden = alertOnly;
  }

  modal.hidden = false;
  document.body.classList.add("confirm-open");

  return new Promise((resolve) => {
    _confirmResolver = resolve;
    _confirmKeyHandler = (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        closeConfirmDialog(alertOnly ? true : false);
      } else if (e.key === "Enter") {
        e.preventDefault();
        closeConfirmDialog(true);
      }
    };
    document.addEventListener("keydown", _confirmKeyHandler);
    setTimeout(() => (btnOk || btnCancel)?.focus(), 30);
  });
}

function appAlert(message, opts = {}) {
  return confirmDialog({
    title: opts.title || "提示",
    message: String(message || ""),
    confirmText: opts.okText || "知道了",
    alertOnly: true,
    danger: !!opts.danger,
  });
}

/** Disable buttons while an async action runs (prevents double-submit / frozen feel). */
async function withBusy(buttons, fn) {
  const list = (Array.isArray(buttons) ? buttons : [buttons]).filter(Boolean);
  const prev = list.map((b) => !!b.disabled);
  list.forEach((b) => {
    b.disabled = true;
    b.classList.add("is-busy");
  });
  try {
    return await fn();
  } finally {
    list.forEach((b, i) => {
      b.disabled = prev[i];
      b.classList.remove("is-busy");
    });
  }
}

function isOverlayBlockingPoll() {
  if ($("stageApp")?.hidden) return true;
  if (state.page !== "tasks") return true;
  if ($("createModal") && !$("createModal").hidden) return true;
  if ($("tagsModal") && !$("tagsModal").hidden) return true;
  if ($("tagPickerModal") && !$("tagPickerModal").hidden) return true;
  if ($("accountDrawer") && !$("accountDrawer").hidden) return true;
  if ($("confirmModal") && !$("confirmModal").hidden) return true;
  if ($("queueModal") && !$("queueModal").hidden) return true;
  if ($("historyPreviewModal") && !$("historyPreviewModal").hidden) return true;
  return false;
}

const WEB_AUTH_HINT_KEY = "tgdl_web_authed";
const TASKS_HTML_CACHE_KEY = "tgdl_tasks_html_v6";

function markWebAuthedHint() {
  try {
    localStorage.setItem(WEB_AUTH_HINT_KEY, "1");
  } catch (_) {}
  document.documentElement.classList.add("boot-app");
  document.documentElement.classList.remove("boot-pending");
}

function clearWebAuthedHint() {
  try {
    localStorage.removeItem(WEB_AUTH_HINT_KEY);
  } catch (_) {}
  document.documentElement.classList.remove("boot-app");
  document.documentElement.classList.remove("boot-pending");
}

function hasWebAuthedHint() {
  try {
    return localStorage.getItem(WEB_AUTH_HINT_KEY) === "1";
  } catch (_) {
    return false;
  }
}

function cacheTaskListHtml(html) {
  try {
    if (html && html.includes('data-task-id')) {
      sessionStorage.setItem(TASKS_HTML_CACHE_KEY, html);
    }
  } catch (_) {}
}

function restoreTaskListCache() {
  const list = $("taskList");
  if (!list || list.querySelector(".task[data-task-id]")) return false;
  try {
    const html = sessionStorage.getItem(TASKS_HTML_CACHE_KEY);
    if (!html) return false;
    list.innerHTML = html;
    bindTaskActions(list);
    return true;
  } catch (_) {
    return false;
  }
}

function toast(text, type = "info", ms = 3400) {
  if (text && /需要 Web 登录|未登录|会话/.test(String(text)) && type === "err") {
    // Soft auth noise — never spam during iOS wake
    if (state._suppressAuthToasts) return;
  }
  const host = $("toastHost");
  if (!host) {
    if (type === "err" && isSoftAuthError({ message: text })) return;
    appAlert(text, { title: type === "err" ? "错误" : "提示" });
    return;
  }
  const kind = normalizeMsgType(text, type);
  const msg = String(text || "");
  // Dedupe: replace same toast instead of stacking
  const twin = [...host.querySelectorAll(".toast")].find(
    (t) => t.querySelector(".toast-text")?.textContent === msg
  );
  if (twin) {
    twin.classList.add("show");
    return;
  }
  while (host.children.length >= 3) host.firstElementChild?.remove();
  const el = document.createElement("div");
  el.className = `toast toast-${kind}`;
  el.setAttribute("role", "status");
  el.innerHTML = `
    <span class="msg-icon" aria-hidden="true"></span>
    <span class="toast-text"></span>
    <button type="button" class="toast-close" aria-label="关闭">×</button>
  `;
  el.querySelector(".toast-text").textContent = msg;
  const close = () => {
    el.classList.remove("show");
    window.setTimeout(() => el.remove(), 220);
  };
  el.querySelector(".toast-close").addEventListener("click", close);
  host.appendChild(el);
  requestAnimationFrame(() => el.classList.add("show"));
  if (ms > 0) window.setTimeout(close, ms);
}

function syncBodyModalLock() {
  const any =
    ($("createModal") && !$("createModal").hidden) ||
    ($("queueModal") && !$("queueModal").hidden) ||
    ($("historyPreviewModal") && !$("historyPreviewModal").hidden);
  document.body.classList.toggle("modal-open", !!any);
}

function closeAllOverlays() {
  try {
    closeConfirmDialog(false);
  } catch (_) {}
  try {
    closeTagPicker();
  } catch (_) {}
  try {
    closeTaskTagsModal();
  } catch (_) {}
  try {
    closeQueueModal();
  } catch (_) {}
  try {
    closeHistoryPreview();
  } catch (_) {}
  try {
    closeAccountDrawer();
  } catch (_) {}
  try {
    closeCreateModal();
  } catch (_) {}
  document.body.classList.remove(
    "modal-open",
    "confirm-open",
    "drawer-open",
    "under-picker"
  );
  document.querySelectorAll(".is-loading").forEach((el) => {
    el.classList.remove("is-loading");
  });
}

function showWebLogin() {
  clearWebAuthedHint();
  closeAllOverlays();
  stopKeepalive();
  $("stageLogin").hidden = false;
  $("stageApp").hidden = true;
  document.body.classList.add("is-login-stage");
  stopPolling();
}

function showApp() {
  markWebAuthedHint();
  $("stageLogin").hidden = true;
  $("stageApp").hidden = false;
  document.body.classList.remove("is-login-stage");
}

function setTgBanner(show) {
  const el = $("tgBanner");
  if (el) el.hidden = !show;
}

function setTgSub(step) {
  state.tgStep = step;
  const api = $("tgSubApi");
  const phone = $("tgSubPhone");
  const code = $("tgSubCode");
  if (!api) return;
  api.hidden = step !== "api";
  phone.hidden = step !== "phone";
  code.hidden = step !== "code";
  $("tgProgApi").classList.toggle("on", step === "api");
  $("tgProgApi").classList.toggle("done", step === "phone" || step === "code");
  $("tgProgPhone").classList.toggle("on", step === "phone");
  $("tgProgPhone").classList.toggle("done", step === "code");
  $("tgProgCode").classList.toggle("on", step === "code");
}

function initials(name) {
  const s = String(name || "TG").trim();
  if (!s) return "TG";
  // CJK: one character looks cleaner in the avatar
  if (/[\u3400-\u9fff]/.test(s[0])) return s[0];
  const parts = s.replace(/^@/, "").split(/\s+/).filter(Boolean);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return s.slice(0, 2).toUpperCase();
}

function bindUi() {
  $("formWebAuth").addEventListener("submit", async (e) => {
    e.preventDefault();
    const username = $("webUsername").value.trim();
    const password = $("webPassword").value;
    const submitBtn = e.submitter || $("formWebAuth")?.querySelector('button[type="submit"]');
    await withBusy(submitBtn, async () => {
      setMsg($("webAuthMsg"), "登录中…");
      try {
        const r = await api("/api/auth/web-login", {
          method: "POST",
          body: JSON.stringify({ username, password }),
        });
        if (!r.ok) {
          setMsg($("webAuthMsg"), r.message || "账号或密码错误", "err");
          return;
        }
        state.webUsername = r.username || username || null;
        markWebAuthedHint();
        setMsg($("webAuthMsg"), "登录成功", "ok");
        // Enter UI immediately — do not wait for Telegram / task list
        enterApp({ background: true });
      } catch (err) {
        setMsg($("webAuthMsg"), err.message || String(err), "err");
      }
    });
  });

  $("btnOpenAccount").addEventListener("click", () => openSettings());
  $("btnCloseAccount").addEventListener("click", closeAccountDrawer);
  $("accountBackdrop").addEventListener("click", closeAccountDrawer);
  $("btnBannerOpenSettings").addEventListener("click", () => openSettings(true));

  $("btnNavTasks")?.addEventListener("click", () => switchPage("tasks"));
  $("btnNavHistory")?.addEventListener("click", () => switchPage("history"));
  $("btnOpenCreate")?.addEventListener("click", () => openCreateModal());
  $("btnCloseCreate")?.addEventListener("click", closeCreateModal);
  $("btnCancelCreate")?.addEventListener("click", closeCreateModal);
  $("createBackdrop")?.addEventListener("click", closeCreateModal);

  document.addEventListener("keydown", (e) => {
    if (e.key !== "Escape") return;
    const pickerEl = $("tagPickerModal");
    if (pickerEl && !pickerEl.hidden) {
      closeTagPicker();
      return;
    }
    const tagsEl = $("tagsModal");
    if (tagsEl && !tagsEl.hidden) {
      closeTaskTagsModal();
      return;
    }
    const queueModal = $("queueModal");
    if (queueModal && !queueModal.hidden) {
      closeQueueModal();
      return;
    }
    const histPrev = $("historyPreviewModal");
    if (histPrev && !histPrev.hidden) {
      closeHistoryPreview();
      return;
    }
    const createEl = $("createModal");
    if (createEl && !createEl.hidden) {
      closeCreateModal();
      return;
    }
    const drawer = $("accountDrawer");
    if (drawer && !drawer.hidden) closeAccountDrawer();
  });

  document.querySelectorAll(".settings-tab").forEach((btn) => {
    btn.addEventListener("click", () => switchSettingsTab(btn.dataset.tab));
  });
  $("btnRefreshTgStatus").addEventListener("click", () => refreshSettingsPanel());
  $("btnReconnectTg").addEventListener("click", reconnectTelegram);
  $("btnBlAdd")?.addEventListener("click", () => addBlacklistTag());
  $("btnBlRefresh")?.addEventListener("click", () => loadTagBlacklist({ render: true }));
  $("btnBlReset")?.addEventListener("click", () => resetBlacklistTags());
  $("btnSaveRuntime")?.addEventListener("click", () => {
    saveRuntimeSettings().catch((e) => setMsg($("runtimeMsg"), e.message || String(e), "err"));
  });
  $("btnHistoryPreviewClose")?.addEventListener("click", closeHistoryPreview);
  $("btnHistoryPreviewDone")?.addEventListener("click", closeHistoryPreview);
  $("historyPreviewBackdrop")?.addEventListener("click", closeHistoryPreview);
  $("btnQueueClose")?.addEventListener("click", closeQueueModal);
  $("btnQueueDone")?.addEventListener("click", closeQueueModal);
  $("queueBackdrop")?.addEventListener("click", closeQueueModal);
  $("btnQueueRefresh")?.addEventListener("click", () => {
    if (state._queueTaskId) {
      openTaskFilesModal(state._queueTaskId, state._filesKind || "queue");
    }
  });
  $("historyList")?.addEventListener("click", (e) => {
    const item = e.target.closest(".history-item[data-id]");
    if (!item) return;
    const id = Number(item.getAttribute("data-id"));
    const kind = item.getAttribute("data-kind") || "file";
    const previewable = item.getAttribute("data-previewable") === "1";
    const name = item.getAttribute("data-name") || "";
    if (!id) return;
    if (previewable) {
      openHistoryPreview(id, kind, name);
    } else {
      window.open(`/api/history/${id}/file`, "_blank", "noopener");
    }
  });
  $("blTagInput")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      addBlacklistTag();
    }
  });
  $("blTagList")?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-bl-del]");
    if (!btn) return;
    removeBlacklistTag(btn.getAttribute("data-bl-del") || "");
  });

  $("btnTestConn").addEventListener("click", testConnection);
  $("btnTgNextPhone").addEventListener("click", () => {
    if (!$("apiId").value || !$("apiHash").value) {
      setMsg($("tgAuthMsg"), "请填写 API ID 与 API Hash", "err");
      return;
    }
    setMsg($("tgAuthMsg"), "");
    setTgSub("phone");
  });
  $("btnTgBackApi").addEventListener("click", () => setTgSub("api"));
  $("btnTgBackPhone").addEventListener("click", () => setTgSub("phone"));
  $("btnSendCode").addEventListener("click", sendCode);
  $("btnSignIn").addEventListener("click", signIn);

  $("btnAccTestProxy").addEventListener("click", testAccountProxy);
  $("btnAccSaveProxy").addEventListener("click", saveAccountProxy);
  $("btnLogoutTg").addEventListener("click", logoutTelegram);
  $("btnLogoutWeb").addEventListener("click", logoutWeb);
  $("btnWebChangePwd").addEventListener("click", changeWebPassword);
  $("btnWebAddUser").addEventListener("click", addWebUser);

  $("btnToggleChats").addEventListener("click", () => {
    const body = $("chatDropdownBody");
    const expanded = body.classList.contains("collapsed");
    body.classList.toggle("collapsed", !expanded);
    $("btnToggleChats").setAttribute("aria-expanded", expanded ? "true" : "false");
  });
  $("chatSearch").addEventListener("focus", () => {
    if ($("chatDropdownBody").classList.contains("collapsed")) {
      expandChatDropdown();
    }
  });
  $("btnRefreshChats").addEventListener("click", () => {
    expandChatDropdown();
    withBusy($("btnRefreshChats"), () =>
      loadChats({ force: true }).catch((e) => toast(e.message || String(e), "err"))
    );
  });
  $("btnRefreshTasks").addEventListener("click", () =>
    withBusy($("btnRefreshTasks"), () => loadTasks({ force: true }))
  );
  $("tagsModalTaskSelect")?.addEventListener("change", async (e) => {
    const id = e.target && e.target.value;
    if (id) await openTaskTagsModal(id, { keepOpen: true });
  });
  $("btnCreateTask").addEventListener("click", () => createTask());
  $("chatSearch").addEventListener("input", () => {
    clearTimeout(state._chatSearchTimer);
    state._chatSearchTimer = setTimeout(() => {
      renderChats($("chatSearch").value);
    }, 120);
  });

  const folderModeEl = $("folderMode");
  if (folderModeEl) {
    folderModeEl.addEventListener("change", syncFolderModeUi);
    syncFolderModeUi();
  }
  document.querySelectorAll("#downloadModeTabs .mode-tab").forEach((btn) => {
    btn.addEventListener("click", () => setDownloadMode(btn.dataset.mode || "sequential"));
  });
  const btnScan = $("btnIndexScan");
  const btnFull = $("btnIndexFull");
  const btnStop = $("btnIndexStop");
  if (btnScan) btnScan.addEventListener("click", () => startIndexScan(false));
  if (btnFull) {
    btnFull.addEventListener("click", async () => {
      const ok = await confirmDialog({
        title: "全量更新索引",
        message: "将清空该群文案索引并重新全量扫描，确定吗？",
        confirmText: "开始全量更新",
        danger: true,
      });
      if (ok) startIndexScan(true);
    });
  }
  const btnConfirmOk = $("btnConfirmOk");
  const btnConfirmCancel = $("btnConfirmCancel");
  const confirmBackdrop = $("confirmBackdrop");
  if (btnConfirmOk) btnConfirmOk.addEventListener("click", () => closeConfirmDialog(true));
  if (btnConfirmCancel) btnConfirmCancel.addEventListener("click", () => closeConfirmDialog(false));
  if (confirmBackdrop) {
    confirmBackdrop.addEventListener("click", () => {
      const cancelBtn = $("btnConfirmCancel");
      // alert-only: backdrop = dismiss OK; confirm: backdrop = cancel
      closeConfirmDialog(!!(cancelBtn && cancelBtn.hidden));
    });
  }
  const btnTagsClose = $("btnTagsClose");
  const btnTagsModalCancel = $("btnTagsModalCancel");
  const btnTagsModalSave = $("btnTagsModalSave");
  const tagsBackdrop = $("tagsBackdrop");
  if (btnTagsClose) btnTagsClose.addEventListener("click", closeTaskTagsModal);
  if (btnTagsModalCancel) btnTagsModalCancel.addEventListener("click", closeTaskTagsModal);
  if (btnTagsModalSave) btnTagsModalSave.addEventListener("click", () => saveTaskTagsModal());
  if (tagsBackdrop) tagsBackdrop.addEventListener("click", closeTaskTagsModal);
  document.querySelectorAll(".num-stepper").forEach((wrap) => {
    if (wrap.dataset.bound === "1") return;
    wrap.dataset.bound = "1";
    wrap.querySelectorAll(".num-step").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        stepNumberInput(wrap.querySelector('input[type="number"]'), btn.dataset.dir);
      });
    });
  });
  $("tagsModalAutoIndex")?.addEventListener("change", () => {
    const on = !!$("tagsModalAutoIndex")?.checked;
    const sel = $("tagsModalAutoIndexInterval");
    const btn = $("tagsModalAutoIndexIntervalBtn");
    if (sel) sel.disabled = !on;
    if (btn) btn.disabled = !on;
    if (!on) closeAutoIndexIntervalMenu();
  });
  initAutoIndexIntervalDropdown();
  $("btnOpenTagPicker")?.addEventListener("click", () => openTagPicker());
  $("btnTagPickerClose")?.addEventListener("click", closeTagPicker);
  $("btnTagPickerDone")?.addEventListener("click", closeTagPicker);
  $("tagPickerBackdrop")?.addEventListener("click", closeTagPicker);
  $("btnTagPickerAdd")?.addEventListener("click", () => addTagFromPickerInput());
  document.querySelectorAll(".tag-picker-tab[data-picker-page]").forEach((btn) => {
    btn.addEventListener("click", () => switchTagPickerPage(btn.dataset.pickerPage));
  });
  $("tagPickerInput")?.addEventListener("input", () => renderTagPickerSuggest());
  $("tagPickerInput")?.addEventListener("focus", () => renderTagPickerSuggest());
  $("tagPickerInput")?.addEventListener("keydown", (e) => {
    const menu = $("tagPickerSuggestMenu");
    const open = menu && !menu.hidden;
    const items = open ? [...menu.querySelectorAll(".tag-picker-suggest-item")] : [];
    let active = items.findIndex((el) => el.classList.contains("is-active"));
    if (e.key === "ArrowDown" && items.length) {
      e.preventDefault();
      active = active < 0 ? 0 : (active + 1) % items.length;
      items.forEach((el, i) => el.classList.toggle("is-active", i === active));
      items[active]?.scrollIntoView({ block: "nearest" });
      return;
    }
    if (e.key === "ArrowUp" && items.length) {
      e.preventDefault();
      active = active < 0 ? items.length - 1 : (active - 1 + items.length) % items.length;
      items.forEach((el, i) => el.classList.toggle("is-active", i === active));
      items[active]?.scrollIntoView({ block: "nearest" });
      return;
    }
    if (e.key === "Escape" && open) {
      e.preventDefault();
      closeTagPickerSuggest();
      return;
    }
    if (e.key === "Enter") {
      e.preventDefault();
      const pick = items[active] || items[0];
      if (open && pick) {
        pickTagFromSuggest(pick.dataset.tag);
        return;
      }
      addTagFromPickerInput();
    }
  });
  document.addEventListener("click", (e) => {
    const wrap = $("tagPickerSuggest");
    if (wrap && !wrap.contains(e.target)) closeTagPickerSuggest();
  });
  $("tagPickerSearch")?.addEventListener("input", () => {
    clearTimeout(state._tagPickerSearchTimer);
    state._tagPickerSearchTimer = setTimeout(() => renderTagPickerIndex(), 120);
  });
  if (btnStop) btnStop.addEventListener("click", stopIndexScan);
  const btnTagAdd = $("btnTagAdd");
  const tagAddInput = $("tagAddInput");
  if (btnTagAdd) btnTagAdd.addEventListener("click", () => addMonitorTag());
  if (tagAddInput) {
    tagAddInput.addEventListener("input", () => {
      clearTimeout(state._suggestTimer);
      state._suggestTimer = setTimeout(() => refreshTagSuggest(), 160);
    });
    tagAddInput.addEventListener("focus", () => refreshTagSuggest());
    tagAddInput.addEventListener("blur", () => {
      setTimeout(() => hideTagSuggest(), 180);
    });
    tagAddInput.addEventListener("keydown", (e) => {
      const list = state.tagSuggestItems || [];
      if (e.key === "ArrowDown" && list.length) {
        e.preventDefault();
        state.tagSuggestIndex = Math.min(
          list.length - 1,
          (state.tagSuggestIndex < 0 ? -1 : state.tagSuggestIndex) + 1
        );
        renderTagSuggest();
        return;
      }
      if (e.key === "ArrowUp" && list.length) {
        e.preventDefault();
        state.tagSuggestIndex = Math.max(0, state.tagSuggestIndex - 1);
        renderTagSuggest();
        return;
      }
      if (e.key === "Escape") {
        hideTagSuggest();
        return;
      }
      if (e.key === "Enter") {
        e.preventDefault();
        if (state.tagSuggestIndex >= 0 && list[state.tagSuggestIndex]) {
          addMonitorTag(list[state.tagSuggestIndex].tag);
        } else {
          addMonitorTag();
        }
        hideTagSuggest();
      }
    });
  }
  document.addEventListener("click", (e) => {
    const box = e.target && e.target.closest && e.target.closest(".tag-suggest-box");
    if (!box) hideTagSuggest();
  });
  const btnTagEditSave = $("btnTagEditSave");
  const btnTagEditCancel = $("btnTagEditCancel");
  const tagEditInput = $("tagEditInput");
  if (btnTagEditSave) btnTagEditSave.addEventListener("click", saveEditMonitorTag);
  if (btnTagEditCancel) btnTagEditCancel.addEventListener("click", cancelEditMonitorTag);
  if (tagEditInput) {
    tagEditInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        saveEditMonitorTag();
      } else if (e.key === "Escape") {
        cancelEditMonitorTag();
      }
    });
  }
  const tagSearchInput = $("tagSearchInput");
  if (tagSearchInput) {
    tagSearchInput.addEventListener("input", () => {
      state.tagSearch = tagSearchInput.value || "";
      renderTagCloud();
    });
  }
  const kwEl = $("captionKeywords");
  if (kwEl) {
    kwEl.addEventListener("input", () => {
      clearTimeout(state._kwTimer);
      state._kwTimer = setTimeout(() => refreshIndexPreview(), 320);
    });
  }
  renderMonitorTagList();
  setDownloadMode(($("downloadMode") && $("downloadMode").value) || "sequential");
  const btnHist = $("btnRefreshHistory");
  if (btnHist) btnHist.addEventListener("click", () => loadHistory(0));
  const histSearch = $("historySearch");
  if (histSearch) {
    histSearch.addEventListener("input", () => {
      clearTimeout(state.historyTimer);
      state.historyTimer = setTimeout(() => loadHistory(0), 280);
    });
  }
  const btnPrev = $("btnHistoryPrev");
  const btnNext = $("btnHistoryNext");
  if (btnPrev) {
    btnPrev.addEventListener("click", () => {
      loadHistory(Math.max(0, state.historyOffset - state.historyLimit));
    });
  }
  if (btnNext) {
    btnNext.addEventListener("click", () => {
      if (state.historyOffset + state.historyLimit < state.historyTotal) {
        loadHistory(state.historyOffset + state.historyLimit);
      }
    });
  }
}

async function verifyWebSession() {
  return Promise.race([
    api("/api/auth/web-session"),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("会话检查超时")), 2500)
    ),
  ]);
}

async function bootstrap() {
  bindUi();
  const hinted = hasWebAuthedHint();

  // Returning user: paint app + cached cards immediately — no session "确认" wait
  if (hinted) {
    enterApp({ background: true, soft: true });
    verifyWebSession()
      .then((web) => {
        if (web && (web.authenticated || !web.need_password)) {
          state.webUsername = web.username || state.webUsername;
          markWebAuthedHint();
          return;
        }
        // Only kick if server explicitly says unauthenticated (not timeout/network)
        if (web && web.need_password && web.authenticated === false) {
          clearWebAuthedHint();
          showWebLogin();
        }
      })
      .catch(() => {
        /* keep app on timeout / offline — seamless resume */
      });
    return;
  }

  // First visit / logged out: brief blank, then login or app
  try {
    const web = await verifyWebSession();
    if (!web.need_password || web.authenticated) {
      state.webUsername = web.username || null;
      markWebAuthedHint();
      enterApp({ background: true, soft: false });
      return;
    }
    // Explicit unauthenticated
    clearWebAuthedHint();
    showWebLogin();
  } catch (e) {
    // Slow NAS / timeout: if we had a prior hint or cookie may exist, enter soft
    if (e && e.message && /超时/.test(e.message) && hasWebAuthedHint()) {
      enterApp({ background: true, soft: true });
      return;
    }
    if (e && e.message && !/超时/.test(e.message) && !isSoftAuthError(e)) {
      setMsg($("webAuthMsg"), e.message || String(e), "err");
    }
    clearWebAuthedHint();
    showWebLogin();
  }
}

/**
 * Show the app shell immediately.
 * Load download cards FIRST — never block them behind Telegram status/connect.
 */
function enterApp(opts = {}) {
  showApp();
  state.page = "tasks";
  const pageTasks = $("pageTasks");
  const pageHistory = $("pageHistory");
  if (pageTasks) pageTasks.hidden = false;
  if (pageHistory) pageHistory.hidden = true;
  $("btnNavTasks")?.classList.toggle("is-active", true);
  $("btnNavHistory")?.classList.toggle("is-active", false);
  startPolling();

  // Never flash skeleton on refresh — restore cache or leave empty until data arrives
  if (opts.soft) {
    restoreTaskListCache();
  }

  // Tasks first; TG chip / defaults / blacklist in parallel (do not block cards)
  const boot = (async () => {
    try {
      await loadTasks({ force: true });
    } catch (_) {}
    await Promise.allSettled([
      refreshTgState(),
      loadTaskDefaults(),
      loadTagBlacklist({ render: false }).catch(() => {}),
    ]);
  })();
  if (opts.background) {
    boot.catch(() => {});
    return boot;
  }
  return boot;
}

function switchPage(name) {
  const page = name === "history" ? "history" : "tasks";
  state.page = page;
  const pageTasks = $("pageTasks");
  const pageHistory = $("pageHistory");
  if (pageTasks) pageTasks.hidden = page !== "tasks";
  if (pageHistory) pageHistory.hidden = page !== "history";
  $("btnNavTasks")?.classList.toggle("is-active", page === "tasks");
  $("btnNavHistory")?.classList.toggle("is-active", page === "history");
  if (page === "history") {
    loadHistory(state.historyOffset || 0).catch((e) => {
      if (!isSoftAuthError(e)) toast(e.message || String(e), "err");
    });
  } else {
    loadTasks().catch(() => {});
  }
}

async function openCreateModal() {
  closeAccountDrawer();
  const el = $("createModal");
  if (!el) return;
  el.hidden = false;
  syncBodyModalLock();
  setMsg($("taskMsg"), "");
  expandChatDropdown();
  syncFolderModeUi();
  cancelEditMonitorTag();
  renderMonitorTagList();
  setDownloadMode(($("downloadMode") && $("downloadMode").value) || "sequential");
  if (!state.tgAuthorized) {
    setMsg($("taskMsg"), "请先在设置中登录 Telegram", "err");
  } else {
    loadChats().catch((e) => {
      if (!isSoftAuthError(e)) toast(e.message || String(e), "err");
    });
  }
  refreshIndexPanel();
}

function closeCreateModal() {
  const el = $("createModal");
  if (!el) return;
  el.hidden = true;
  syncBodyModalLock();
  stopIndexPolling();
}

async function loadTaskDefaults() {
  const r = await api("/api/tasks/settings/defaults");
  const s = r.settings || {};
  if ($("delayMin") && s.download_delay_min != null) {
    $("delayMin").value = s.download_delay_min;
  }
  if ($("delayMax") && s.download_delay_max != null) {
    $("delayMax").value = s.download_delay_max;
  } else if ($("delayMax") && s.download_delay != null) {
    $("delayMax").value = s.download_delay;
  }
  if ($("testMode") && s.test_mode != null) {
    $("testMode").checked = !!s.test_mode;
  }
}

function syncFolderModeUi() {
  const mode = ($("folderMode") && $("folderMode").value) || "caption";
  const wrap = $("useTextFolderWrap");
  const cb = $("useTextFolder");
  if (!wrap || !cb) return;
  if (mode === "caption") {
    wrap.hidden = false;
    cb.disabled = false;
  } else {
    wrap.hidden = true;
    cb.checked = false;
    cb.disabled = true;
  }
}

function normalizeDownloadMode(mode) {
  const m = String(mode || "sequential").toLowerCase();
  if (m === "monitor" || m === "tags") return "monitor";
  return "sequential"; // all / date / sequential
}

function setDownloadMode(mode) {
  const m = normalizeDownloadMode(mode);
  if ($("downloadMode")) $("downloadMode").value = m;
  document.querySelectorAll("#downloadModeTabs .mode-tab").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.mode === m);
  });
  const datePanel = $("modeDateFields");
  const tagsPanel = $("modeTagsFields");
  const adv = $("advancedIds");
  const hint = $("downloadModeHint");
  const mediaSec = $("createMediaSection");
  const folderSec = $("createFolderSection");
  if (datePanel) datePanel.hidden = m !== "sequential";
  if (tagsPanel) tagsPanel.hidden = m !== "monitor";
  if (adv) adv.hidden = m === "monitor";
  if (mediaSec) mediaSec.hidden = m === "monitor";
  if (folderSec) folderSec.hidden = m === "monitor";
  if (hint) {
    hint.textContent =
      m === "sequential"
        ? "按消息时间顺序扫描并下载；可选用日期范围，下完即结束"
        : "监控模式：为所选群建立文案索引并持续跟踪；标签与下载在任务设置里再配";
  }
  if (m === "monitor") refreshIndexPanel();
  else stopIndexPolling();
}

function stopIndexPolling() {
  state.indexPolling = false;
  if (state.indexTimer) {
    clearTimeout(state.indexTimer);
    state.indexTimer = null;
  }
}

/** Chat used for index scan/meta: settings modal task first, else create-modal selection. */
function getIndexTargetChat() {
  const settingsOpen = $("tagsModal") && !$("tagsModal").hidden;
  if (settingsOpen && state.taskTagsDraft && state.taskTagsDraft.chatId != null) {
    return {
      id: state.taskTagsDraft.chatId,
      title: state.taskTagsDraft.title || "",
    };
  }
  if (state.selectedChats.length === 1) return state.selectedChats[0];
  return null;
}

async function refreshIndexPanel() {
  const metaEl = $("indexMetaText");
  const cloud = $("indexTagCloud");
  const chat = getIndexTargetChat();
  if (!chat) {
    stopIndexPolling();
    state.indexMeta = null;
    state.indexTags = [];
    state._indexTagsSig = "";
    state._indexWasScanning = false;
    if (metaEl) {
      metaEl.textContent =
        state.selectedChats.length > 1
          ? "多选群组时请分别建任务后再在设置里更新索引"
          : "打开任务设置后可更新该群文案索引";
    }
    if (cloud) cloud.innerHTML = "";
    if ($("indexPreview")) $("indexPreview").textContent = "";
    return;
  }
  try {
    const r = await api(`/api/index/${encodeURIComponent(chat.id)}`);
    state.indexMeta = r.meta || null;
    state.indexTags = r.tags || [];
    state.tagRelated = r.related || {};
    state.indexCoverage = r.coverage || null;
    const scanning = !!(r.scanning || (r.meta && r.meta.status === "scanning"));
    const tagsSig = JSON.stringify(
      (state.indexTags || []).map((t) => `${t.tag || t}:${t.count || 0}`)
    );
    renderIndexMeta(scanning);
    // Avoid rebuilding the whole tag cloud every poll tick while scanning
    if (!scanning || tagsSig !== state._indexTagsSig || !state._indexWasScanning) {
      renderTagCloud();
    }
    state._indexTagsSig = tagsSig;
    state._indexWasScanning = scanning;
    if (scanning) scheduleIndexPoll();
    else stopIndexPolling();
    refreshIndexPreview();
  } catch (e) {
    if (isSoftAuthError(e)) return;
    if (metaEl && !(state.indexMeta)) {
      metaEl.textContent = e.message || "加载索引失败";
    }
  }
}

function renderIndexMeta(scanning) {
  const metaEl = $("indexMetaText");
  const btnStop = $("btnIndexStop");
  if (!metaEl) return;
  const m = state.indexMeta || {};
  const cov = state.indexCoverage || {};
  const count = m.media_count ?? 0;
  const scanned = m.scanned_count ?? 0;
  const last = m.last_scan_at
    ? String(m.last_scan_at).replace("T", " ").slice(0, 19)
    : "尚未扫描";
  const coverHint =
    cov.complete === true
      ? " · 已覆盖全群"
      : cov.complete === false
        ? ` · 未全覆盖（落后 ${cov.behind || "?"}）`
        : "";
  const autoOn = !!Number(m.auto_incremental);
  const autoMin = Number(m.auto_interval_min) || 60;
  const autoHint = autoOn ? ` · 自动增量每 ${autoMin} 分钟` : "";
  if (scanning || m.status === "scanning") {
    metaEl.textContent = `扫描中… 已看 ${scanned} 条消息，已存 ${count} 条媒体文案${
      m.last_error ? `（${m.last_error}）` : ""
    }${autoHint}`;
  } else if (m.status === "error") {
    metaEl.textContent = `索引出错：${m.last_error || "未知错误"} · 已存 ${count} 条`;
  } else {
    metaEl.textContent = `上次扫描 ${last} · 已存 ${count} 条媒体完整文案${coverHint}${autoHint}`;
  }
  if (btnStop) btnStop.hidden = !(scanning || m.status === "scanning");
  syncAutoIndexControls(m);
}

function syncAutoIndexControls(meta) {
  const m = meta || state.indexMeta || {};
  const chk = $("tagsModalAutoIndex");
  const sel = $("tagsModalAutoIndexInterval");
  const btn = $("tagsModalAutoIndexIntervalBtn");
  if (chk) chk.checked = !!Number(m.auto_incremental);
  if (sel) {
    const v = String(Number(m.auto_interval_min) || 60);
    if ([...sel.options].some((o) => o.value === v)) sel.value = v;
    else sel.value = "60";
    const on = chk ? !!chk.checked : false;
    sel.disabled = !on;
    if (btn) btn.disabled = !on;
    syncAutoIndexIntervalUi(sel.value);
  }
}

function autoIndexIntervalLabel(value) {
  const sel = $("tagsModalAutoIndexInterval");
  const opt = sel && [...sel.options].find((o) => o.value === String(value));
  return opt ? opt.textContent : "1 小时";
}

function syncAutoIndexIntervalUi(value) {
  const v = String(value || "60");
  const label = $("tagsModalAutoIndexIntervalLabel");
  const menu = $("tagsModalAutoIndexIntervalMenu");
  if (label) label.textContent = autoIndexIntervalLabel(v);
  if (!menu) return;
  menu.querySelectorAll("[data-value]").forEach((btn) => {
    btn.setAttribute("aria-selected", btn.dataset.value === v ? "true" : "false");
  });
}

function closeAutoIndexIntervalMenu() {
  const menu = $("tagsModalAutoIndexIntervalMenu");
  const btn = $("tagsModalAutoIndexIntervalBtn");
  if (menu) menu.hidden = true;
  if (btn) btn.setAttribute("aria-expanded", "false");
}

function initAutoIndexIntervalDropdown() {
  const root = $("tagsModalAutoIndexDd");
  const sel = $("tagsModalAutoIndexInterval");
  const btn = $("tagsModalAutoIndexIntervalBtn");
  const menu = $("tagsModalAutoIndexIntervalMenu");
  if (!root || !sel || !btn || !menu || root.dataset.bound) return;
  root.dataset.bound = "1";

  btn.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (btn.disabled) return;
    const open = menu.hidden;
    closeAutoIndexIntervalMenu();
    if (open) {
      menu.hidden = false;
      btn.setAttribute("aria-expanded", "true");
    }
  });

  menu.addEventListener("click", (e) => {
    const opt = e.target.closest("[data-value]");
    if (!opt) return;
    e.preventDefault();
    e.stopPropagation();
    sel.value = opt.dataset.value;
    syncAutoIndexIntervalUi(sel.value);
    closeAutoIndexIntervalMenu();
    sel.dispatchEvent(new Event("change", { bubbles: true }));
  });

  document.addEventListener("click", (e) => {
    if (!root.contains(e.target)) closeAutoIndexIntervalMenu();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAutoIndexIntervalMenu();
  });
  syncAutoIndexIntervalUi(sel.value || "60");
}

function normalizeTagName(raw) {
  return String(raw || "")
    .trim()
    .replace(/^#+/, "")
    .replace(/\s+/g, " ");
}

function syncIncludeTagsHidden() {
  if ($("includeTags")) {
    $("includeTags").value = (state.monitorTags || [])
      .map((t) => `#${t}`)
      .join(", ");
  }
  const countEl = $("monitorTagCount");
  if (countEl) countEl.textContent = `已选 ${(state.monitorTags || []).length}`;
  const emptyEl = $("monitorTagEmpty");
  if (emptyEl) emptyEl.hidden = (state.monitorTags || []).length > 0;
}

function getMonitorTags() {
  return Array.isArray(state.monitorTags) ? state.monitorTags.slice() : [];
}

function isTagAutoRelatedEnabled() {
  const el = $("tagAutoRelated");
  return !el || !!el.checked;
}

function lookupRelatedTags(tag) {
  const name = normalizeTagName(tag);
  if (!name) return [];
  const map = state.tagRelated || {};
  if (Array.isArray(map[name])) return map[name].slice();
  const key = name.toLowerCase();
  for (const [k, v] of Object.entries(map)) {
    if (String(k).toLowerCase() === key && Array.isArray(v)) return v.slice();
  }
  // fallback from indexTags suggest cache
  const hit = (state.tagSuggestItems || []).find(
    (x) => String(x.tag || "").toLowerCase() === key
  );
  return hit && Array.isArray(hit.related) ? hit.related.slice() : [];
}

function renderMonitorTagList() {
  const listEl = $("monitorTagList");
  if (!listEl) return;
  syncIncludeTagsHidden();
  const tags = state.monitorTags || [];
  const meta = state.monitorTagMeta || {};
  if (!tags.length) {
    listEl.innerHTML = "";
    return;
  }
  listEl.innerHTML = tags
    .map((tag, idx) => {
      const editing = state.editingTagIndex === idx ? "is-editing" : "";
      const info = meta[tag.toLowerCase()] || {};
      const related = info.related ? "is-related" : "";
      const mark = info.related
        ? `<span class="tag-related-mark" title="自动关联">关</span>`
        : "";
      return `<div class="monitor-tag-chip ${editing} ${related}" data-idx="${idx}">
        ${mark}<span class="monitor-tag-name">#${escapeHtml(tag)}</span>
        <button type="button" class="tag-chip-btn tag-chip-edit" data-action="edit" data-idx="${idx}" title="修改">改</button>
        <button type="button" class="tag-chip-btn tag-chip-del" data-action="del" data-idx="${idx}" title="删除">删</button>
      </div>`;
    })
    .join("");
  listEl.querySelectorAll("[data-action]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const idx = Number(btn.dataset.idx);
      if (btn.dataset.action === "edit") startEditMonitorTag(idx);
      else if (btn.dataset.action === "del") removeMonitorTag(idx);
    });
  });
  syncTagCloudSelection();
}

function commitMonitorTags(name, relatedList) {
  const existing = new Set((state.monitorTags || []).map((t) => t.toLowerCase()));
  const toAdd = [];
  if (!existing.has(name.toLowerCase())) {
    toAdd.push({ tag: name, related: false, seed: true });
  }
  const relatedAdded = [];
  for (const rel of relatedList || []) {
    const rname = normalizeTagName(rel);
    if (!rname) continue;
    const key = rname.toLowerCase();
    if (existing.has(key) || toAdd.some((x) => x.tag.toLowerCase() === key)) continue;
    toAdd.push({ tag: rname, related: true, seed: false });
    relatedAdded.push(rname);
  }
  if (!toAdd.length) {
    toast(`标签 #${name} 已在列表中`, "err");
    return false;
  }
  state.monitorTagMeta = state.monitorTagMeta || {};
  for (const item of toAdd) {
    state.monitorTags = [...(state.monitorTags || []), item.tag];
    state.monitorTagMeta[item.tag.toLowerCase()] = {
      related: !!item.related,
      seed: !!item.seed,
    };
    existing.add(item.tag.toLowerCase());
  }
  const input = $("tagAddInput");
  if (input) input.value = "";
  hideTagSuggest();
  cancelEditMonitorTag();
  renderMonitorTagList();
  refreshIndexPreview();
  if (relatedAdded.length) {
    toast(
      `已添加 #${name}，并自动关联 ${relatedAdded.map((t) => "#" + t).join(" ")}`,
      "ok"
    );
  }
  return true;
}

function addMonitorTag(raw, opts = {}) {
  const input = $("tagAddInput");
  const name = normalizeTagName(raw != null ? raw : input && input.value);
  const withRelated = opts.withRelated !== false && isTagAutoRelatedEnabled();
  if (!name) {
    toast("请输入标签名", "err");
    return false;
  }
  let relatedList = withRelated ? lookupRelatedTags(name) : [];
  // local map miss → fetch API expand (async)
  if (
    withRelated &&
    !relatedList.length &&
    state.selectedChats.length === 1
  ) {
    const chat = state.selectedChats[0];
    api(
      `/api/index/${encodeURIComponent(chat.id)}/tags/related?tag=${encodeURIComponent(name)}`
    )
      .then((r) => {
        const rel = r.related || [];
        if (rel.length) {
          state.tagRelated = state.tagRelated || {};
          state.tagRelated[name] = rel;
        }
        commitMonitorTags(name, rel);
      })
      .catch(() => commitMonitorTags(name, []));
    return true;
  }
  return commitMonitorTags(name, relatedList);
}

async function refreshTagSuggest() {
  const input = $("tagAddInput");
  const listEl = $("tagSuggestList");
  if (!input || !listEl) return;
  if (state.selectedChats.length !== 1) {
    hideTagSuggest();
    return;
  }
  const q = normalizeTagName(input.value);
  const chat = state.selectedChats[0];
  try {
    const r = await api(
      `/api/index/${encodeURIComponent(chat.id)}/tags/suggest?q=${encodeURIComponent(q)}&limit=12`
    );
    state.tagSuggestItems = r.items || [];
    state.tagSuggestIndex = state.tagSuggestItems.length ? 0 : -1;
    // merge related into local map for offline expand
    for (const it of state.tagSuggestItems) {
      if (it.tag && Array.isArray(it.related) && it.related.length) {
        state.tagRelated = state.tagRelated || {};
        state.tagRelated[it.tag] = it.related;
      }
    }
    renderTagSuggest();
  } catch (_) {
    // local fallback filter
    const qq = q.toLowerCase();
    state.tagSuggestItems = (state.indexTags || [])
      .filter((t) => !qq || String(t.tag).toLowerCase().includes(qq))
      .slice(0, 12)
      .map((t) => ({
        tag: t.tag,
        count: t.count,
        related: lookupRelatedTags(t.tag),
        related_count: lookupRelatedTags(t.tag).length,
      }));
    state.tagSuggestIndex = state.tagSuggestItems.length ? 0 : -1;
    renderTagSuggest();
  }
}

function renderTagSuggest() {
  const listEl = $("tagSuggestList");
  if (!listEl) return;
  const items = state.tagSuggestItems || [];
  if (!items.length) {
    listEl.hidden = true;
    listEl.innerHTML = "";
    return;
  }
  listEl.hidden = false;
  listEl.innerHTML = items
    .map((it, i) => {
      const active = i === state.tagSuggestIndex ? "is-active" : "";
      const relN = it.related_count || (it.related || []).length || 0;
      const meta = relN
        ? `${it.count || 0} 条 · 关联 ${relN}`
        : `${it.count || 0} 条`;
      return `<button type="button" class="tag-suggest-item ${active}" data-idx="${i}">
        <span class="suggest-tag">#${escapeHtml(it.tag)}</span>
        <span class="suggest-meta">${escapeHtml(meta)}</span>
      </button>`;
    })
    .join("");
  listEl.querySelectorAll(".tag-suggest-item").forEach((btn) => {
    btn.addEventListener("mousedown", (e) => {
      e.preventDefault();
      const idx = Number(btn.dataset.idx);
      const item = state.tagSuggestItems[idx];
      if (item) addMonitorTag(item.tag);
    });
  });
}

function hideTagSuggest() {
  const listEl = $("tagSuggestList");
  if (listEl) {
    listEl.hidden = true;
    listEl.innerHTML = "";
  }
  state.tagSuggestIndex = -1;
}

function removeMonitorTag(idx) {
  const tags = state.monitorTags || [];
  if (idx < 0 || idx >= tags.length) return;
  if (state.editingTagIndex === idx) cancelEditMonitorTag();
  else if (state.editingTagIndex > idx) state.editingTagIndex -= 1;
  const removed = tags[idx];
  state.monitorTags = tags.filter((_, i) => i !== idx);
  if (removed && state.monitorTagMeta) {
    delete state.monitorTagMeta[removed.toLowerCase()];
  }
  renderMonitorTagList();
  refreshIndexPreview();
}

function startEditMonitorTag(idx) {
  const tags = state.monitorTags || [];
  if (idx < 0 || idx >= tags.length) return;
  state.editingTagIndex = idx;
  const row = $("tagEditRow");
  const input = $("tagEditInput");
  if (row) row.hidden = false;
  if (input) {
    input.value = tags[idx];
    input.focus();
    input.select();
  }
  renderMonitorTagList();
}

function saveEditMonitorTag() {
  const idx = state.editingTagIndex;
  const tags = state.monitorTags || [];
  if (idx < 0 || idx >= tags.length) {
    cancelEditMonitorTag();
    return;
  }
  const name = normalizeTagName($("tagEditInput") && $("tagEditInput").value);
  if (!name) {
    toast("标签名不能为空", "err");
    return;
  }
  const key = name.toLowerCase();
  const dup = tags.some((t, i) => i !== idx && t.toLowerCase() === key);
  if (dup) {
    toast(`标签 #${name} 已存在`, "err");
    return;
  }
  const next = tags.slice();
  next[idx] = name;
  state.monitorTags = next;
  cancelEditMonitorTag();
  renderMonitorTagList();
  refreshIndexPreview();
  toast(`已更新为 #${name}`, "ok");
}

function cancelEditMonitorTag() {
  const wasEditing = state.editingTagIndex >= 0;
  state.editingTagIndex = -1;
  const row = $("tagEditRow");
  if (row) row.hidden = true;
  const input = $("tagEditInput");
  if (input) input.value = "";
  if (wasEditing) renderMonitorTagList();
  else syncIncludeTagsHidden();
}

function renderTagCloud() {
  const cloud = $("indexTagCloud");
  if (!cloud) return;
  const selected = new Set((state.monitorTags || []).map((t) => t.toLowerCase()));
  const q = String(state.tagSearch || "")
    .trim()
    .replace(/^#/, "")
    .toLowerCase();
  const tags = state.indexTags || [];
  if (!tags.length) {
    cloud.innerHTML = `<span class="muted">暂无标签，请先更新索引</span>`;
    return;
  }
  const filtered = q
    ? tags.filter((t) => String(t.tag || "").toLowerCase().includes(q))
    : tags;
  if (!filtered.length) {
    cloud.innerHTML = `<span class="muted">没有匹配「${escapeHtml(state.tagSearch)}」的索引标签</span>`;
    return;
  }
  cloud.innerHTML = filtered
    .slice(0, 100)
    .map((t) => {
      const active = selected.has(String(t.tag).toLowerCase()) ? "is-active" : "";
      const relN = lookupRelatedTags(t.tag).length;
      const rel = relN ? `<span class="tag-count">·${relN}关联</span>` : "";
      return `<button type="button" class="tag-pill ${active}" data-tag="${escapeHtml(t.tag)}">#${escapeHtml(
        t.tag
      )} <span class="tag-count">${t.count}</span>${rel}</button>`;
    })
    .join("");
  cloud.querySelectorAll(".tag-pill").forEach((btn) => {
    btn.addEventListener("click", () => toggleIndexTag(btn.dataset.tag));
  });
}

function syncTagCloudSelection() {
  const selected = new Set((state.monitorTags || []).map((t) => t.toLowerCase()));
  document.querySelectorAll("#indexTagCloud .tag-pill").forEach((btn) => {
    btn.classList.toggle(
      "is-active",
      selected.has(String(btn.dataset.tag || "").toLowerCase())
    );
  });
}

function toggleIndexTag(tag) {
  const name = normalizeTagName(tag);
  if (!name) return;
  const key = name.toLowerCase();
  const list = state.monitorTags || [];
  const i = list.findIndex((t) => t.toLowerCase() === key);
  if (i >= 0) {
    removeMonitorTag(i);
    return;
  }
  addMonitorTag(name);
}

async function refreshIndexPreview() {
  const el = $("indexPreview");
  if (!el) return;
  if (normalizeDownloadMode($("downloadMode") && $("downloadMode").value) !== "monitor") {
    el.textContent = "";
    return;
  }
  if (state.selectedChats.length !== 1) {
    el.textContent = "";
    return;
  }
  const tags = getMonitorTags();
  const q = (($("captionKeywords") && $("captionKeywords").value) || "").trim();
  if (!tags.length && !q) {
    el.textContent = "添加标签或填写关键词后预览命中数量";
    return;
  }
  const chat = state.selectedChats[0];
  const params = new URLSearchParams({
    limit: "3",
    offset: "0",
    tag_match_mode: "any",
  });
  if (tags.length) params.set("tags", tags.join(","));
  if (q) {
    const first = parseCaptionKeywords(q)[0] || q;
    params.set("q", first);
  }
  try {
    const r = await api(`/api/index/${encodeURIComponent(chat.id)}/items?${params}`);
    state.indexPreviewTotal = r.total ?? 0;
    const samples = (r.items || [])
      .map((it) => {
        const cap = (it.caption || "").replace(/\s+/g, " ").trim();
        const short = cap.length > 48 ? `${cap.slice(0, 48)}…` : cap || "(无文案)";
        return short;
      })
      .filter(Boolean);
    el.textContent = samples.length
      ? `预计命中 ${r.total} 条 · 例：${samples.join(" / ")}`
      : `预计命中 ${r.total} 条`;
  } catch (_) {
    el.textContent = "";
  }
}

function scheduleIndexPoll() {
  state.indexPolling = true;
  if (state.indexTimer) clearTimeout(state.indexTimer);
  state.indexTimer = setTimeout(async () => {
    if (!state.indexPolling) return;
    await refreshIndexPanel();
  }, 1200);
}

async function startIndexScan(full) {
  const chat = getIndexTargetChat();
  if (!chat) {
    toast("请先打开任务设置，再更新索引", "err");
    return;
  }
  const btns = [
    $("btnIndexScan"),
    $("btnIndexFull"),
    $("btnIndexStop"),
  ].filter(Boolean);
  await withBusy(btns, async () => {
    try {
      const r = await api(`/api/index/${encodeURIComponent(chat.id)}/scan`, {
        method: "POST",
        body: JSON.stringify({ full: !!full, chat_title: chat.title || "" }),
      });
      if (!r.ok) {
        toast(r.message || "开始扫描失败", "err");
        return;
      }
      state.indexMeta = r.meta || state.indexMeta;
      state.indexTags = r.tags || state.indexTags;
      state._indexWasScanning = false; // force cloud refresh once
      renderIndexMeta(true);
      renderTagCloud();
      scheduleIndexPoll();
      toast(full ? "已开始全量更新" : "已开始增量更新", "ok");
    } catch (e) {
      toast(e.message || String(e), "err");
    }
  });
}

async function stopIndexScan() {
  const chat = getIndexTargetChat();
  if (!chat) return;
  await withBusy([$("btnIndexStop"), $("btnIndexScan"), $("btnIndexFull")], async () => {
    try {
      await api(`/api/index/${encodeURIComponent(chat.id)}/stop`, {
        method: "POST",
        body: "{}",
      });
      stopIndexPolling();
      await refreshIndexPanel();
      toast("已请求停止扫描", "ok");
    } catch (e) {
      toast(e.message || String(e), "err");
    }
  });
}

function parseFileFormats(raw) {
  return String(raw || "")
    .split(/[,，\s]+/)
    .map((x) => x.trim().replace(/^\./, "").toLowerCase())
    .filter(Boolean);
}

function buildFileFormatsPayload() {
  const byType = {};
  const map = [
    ["video", "fmtVideo"],
    ["photo", "fmtPhoto"],
    ["document", "fmtDocument"],
    ["audio", "fmtAudio"],
  ];
  for (const [key, id] of map) {
    const list = parseFileFormats($(id) && $(id).value);
    if (list.length) byType[key] = list;
  }
  if (Object.keys(byType).length) return byType;
  return parseFileFormats($("fileFormats") && $("fileFormats").value);
}

function mbToBytes(raw) {
  const s = String(raw == null ? "" : raw).trim();
  if (!s) return 0;
  const n = Number(s);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.round(n * 1024 * 1024);
}

function parseIncludeTags(raw) {
  return String(raw || "")
    .split(/[,，\s]+/)
    .map((x) => x.trim().replace(/^#/, ""))
    .filter(Boolean);
}

function parseCaptionKeywords(raw) {
  return String(raw || "")
    .split(/[,，]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function buildTaskOptions() {
  const media = [...document.querySelectorAll('input[name="media"]:checked')].map((x) => x.value);
  const endVal = $("endMsgId").value;
  const startDate = $("startDate").value;
  const endDate = $("endDate").value;
  const maxRaw = $("maxMessages") ? $("maxMessages").value.trim() : "";
  const folderMode = ($("folderMode") && $("folderMode").value) || "caption";
  let delayMin = Number(($("delayMin") && $("delayMin").value) || 0.5);
  let delayMax = Number(($("delayMax") && $("delayMax").value) || delayMin);
  if (Number.isNaN(delayMin) || delayMin < 0) delayMin = 0;
  if (Number.isNaN(delayMax) || delayMax < delayMin) delayMax = delayMin;
  const downloadMode = normalizeDownloadMode(
    ($("downloadMode") && $("downloadMode").value) || "sequential"
  );
  const includeTags = downloadMode === "monitor" ? getMonitorTags() : [];
  const captionKeywords =
    downloadMode === "monitor"
      ? parseCaptionKeywords($("captionKeywords") && $("captionKeywords").value)
      : [];
  return {
    media_types: media,
    use_text_as_folder: folderMode === "caption" ? ($("useTextFolder") ? $("useTextFolder").checked : true) : false,
    test_mode: $("testMode").checked,
    min_folder_title_len: Number(($("minTitleLen") && $("minTitleLen").value) || 2),
    start_message_id: Number(($("startMsgId") && $("startMsgId").value) || 0),
    end_message_id: endVal === "" ? null : Number(endVal),
    start_date: downloadMode === "sequential" ? startDate || null : null,
    end_date: downloadMode === "sequential" ? endDate || null : null,
    download_order: ($("downloadOrder") && $("downloadOrder").value) || "added_first",
    concurrency: Math.max(1, Math.min(5, Number(($("concurrency") && $("concurrency").value) || 2))),
    file_formats: buildFileFormatsPayload(),
    min_file_bytes: mbToBytes($("minFileMb") && $("minFileMb").value),
    max_file_bytes: mbToBytes($("maxFileMb") && $("maxFileMb").value),
    max_messages: maxRaw === "" ? null : Math.max(1, Number(maxRaw) || 1),
    delay_min: delayMin,
    delay_max: delayMax,
    folder_mode: folderMode,
    include_tags: includeTags,
    caption_keywords: captionKeywords,
    tag_match_mode: "any",
    download_mode: downloadMode,
    use_index: downloadMode === "monitor",
    auto_start: true,
  };
}

async function refreshTgState() {
  try {
    const st = await api("/api/auth/status");
    const prev = state._tgLastOk;
    // Soft timeout / connecting without user must not wipe a known-good online chip
    if (
      prev &&
      prev.authorized &&
      st &&
      !st.authorized &&
      (st.connecting || /连接中|超时|timeout/i.test(String(st.message || "")))
    ) {
      applyAccountChip({ ...prev, connecting: true });
      setTgBanner(false);
      if (st.proxy && $("proxy")) $("proxy").value = st.proxy;
      if (st.proxy && $("accProxy")) $("accProxy").value = st.proxy;
      return prev;
    }
    state.tgAuthorized = !!st.authorized;
    if (st.authorized || !prev?.authorized) {
      state._tgLastOk = st;
    }
    applyAccountChip(st);
    // Soft-connecting: keep banner hidden if session exists
    setTgBanner(!st.authorized && !st.connecting);
    if (st.proxy && $("proxy")) $("proxy").value = st.proxy;
    if (st.proxy && $("accProxy")) $("accProxy").value = st.proxy;
    return st;
  } catch (e) {
    if (isSoftAuthError(e)) return state._tgLastOk || null;
    // Keep last good status on transient network errors (iOS tab wake)
    if (state._tgLastOk && state._tgLastOk.authorized) {
      return state._tgLastOk;
    }
    state.tgAuthorized = false;
    setTgBanner(true);
    applyAccountChip({ authorized: false });
    return null;
  }
}

function applyAccountChip(st) {
  const u = (st && st.user) || {};
  const chip = $("btnOpenAccount");
  const sub = $("userBadgeSub");
  if (st && st.authorized) {
    const connecting = !!st.connecting && !u.first_name && !u.username;
    const name = connecting
      ? "连接中…"
      : u.first_name || u.username || (u.phone ? `+${u.phone}` : "已连接");
    $("userBadge").textContent = name;
    if (sub) {
      sub.textContent = connecting
        ? "正在连 Telegram"
        : u.username
          ? `@${u.username}`
          : "已连接 · 设置";
    }
    const av = initials(u.first_name || u.username || "TG");
    $("accountAvatar").textContent = av;
    if ($("accountAvatarLg")) $("accountAvatarLg").textContent = av;
    if (chip) {
      chip.classList.toggle("is-online", !connecting);
      chip.classList.toggle("is-offline", connecting);
    }
  } else {
    $("userBadge").textContent = "未连接";
    if (sub) sub.textContent = "打开设置";
    $("accountAvatar").textContent = "TG";
    if ($("accountAvatarLg")) $("accountAvatarLg").textContent = "TG";
    if (chip) {
      chip.classList.add("is-offline");
      chip.classList.remove("is-online");
    }
  }
}

function switchSettingsTab(tab) {
  const name =
    tab === "web" ? "web" : tab === "tags" ? "tags" : tab === "runtime" ? "runtime" : "telegram";
  const btnTg = $("tabBtnTelegram");
  const btnRuntime = $("tabBtnRuntime");
  const btnTags = $("tabBtnTags");
  const btnWeb = $("tabBtnWeb");
  const panelTg = $("tabTelegram");
  const panelRuntime = $("tabRuntime");
  const panelTags = $("tabTags");
  const panelWeb = $("tabWeb");
  if (!btnTg || !btnWeb || !panelTg || !panelWeb) return;

  const map = [
    ["telegram", btnTg, panelTg],
    ["runtime", btnRuntime, panelRuntime],
    ["tags", btnTags, panelTags],
    ["web", btnWeb, panelWeb],
  ];
  for (const [key, btn, panel] of map) {
    if (!btn || !panel) continue;
    const on = key === name;
    btn.classList.toggle("is-active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
    panel.hidden = !on;
  }
  if (name === "tags") {
    loadTagBlacklist({ render: true });
  }
  if (name === "runtime") {
    loadRuntimeSettings().catch(() => {});
  }
}

async function loadRuntimeSettings() {
  const r = await api("/api/settings/runtime");
  if ($("maxParallelChats")) {
    $("maxParallelChats").value = String(r.max_parallel_chats || 1);
  }
  if ($("notifyEnabled")) $("notifyEnabled").checked = !!r.notify_enabled;
  if ($("notifyWebhook")) $("notifyWebhook").value = r.notify_webhook || "";
  const hint = $("runtimeLogHint");
  if (hint && r.log_dir) {
    hint.textContent = `日志目录：${r.log_dir}`;
  }
}

async function saveRuntimeSettings() {
  const parallel = Math.max(1, Math.min(10, Number(($("maxParallelChats") && $("maxParallelChats").value) || 1)));
  const body = {
    max_parallel_chats: parallel,
    notify_enabled: !!($("notifyEnabled") && $("notifyEnabled").checked),
    notify_webhook: ($("notifyWebhook") && $("notifyWebhook").value.trim()) || "",
  };
  const r = await api("/api/settings/runtime", { method: "PUT", body: JSON.stringify(body) });
  if ($("maxParallelChats")) $("maxParallelChats").value = String(r.max_parallel_chats || parallel);
  setMsg($("runtimeMsg"), "已保存", "ok");
}

async function openSettings(focusTgLogin = false) {
  closeCreateModal();
  $("accountDrawer").hidden = false;
  document.body.classList.add("drawer-open");
  switchSettingsTab(focusTgLogin ? "telegram" : "telegram");
  setTgSub(state.tgAuthorized ? "api" : state.tgStep || "api");
  await refreshSettingsPanel(focusTgLogin);
}

async function refreshSettingsPanel(focusTgLogin = false) {
  try {
    const r = await api("/api/auth/account");
    state.account = r;
    fillAccountPanel(r);
    const loginBlock = $("tgSettingsLogin");
    if (loginBlock) loginBlock.hidden = !!(r.telegram && r.telegram.authorized);
    if (focusTgLogin && loginBlock && !loginBlock.hidden) {
      loginBlock.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    const tagsPanel = $("tabTags");
    if (tagsPanel && !tagsPanel.hidden) {
      await loadTagBlacklist({ render: true });
    }
  } catch (e) {
    setMsg($("accProxyMsg"), e.message || String(e), "err");
  }
}

async function reconnectTelegram() {
  await withBusy($("btnReconnectTg"), async () => {
    setMsg($("tgReconnectMsg"), "正在重新连接…", "info");
    try {
      const r = await api("/api/auth/reconnect", { method: "POST", body: "{}" });
      if (r.ok) {
        setMsg($("tgReconnectMsg"), "已重新连接 Telegram", "ok");
        toast("Telegram 已重新连接", "ok");
      } else {
        setMsg($("tgReconnectMsg"), r.message || r.reason || "重连失败", "err");
      }
      await refreshSettingsPanel();
      await refreshTgState();
    } catch (e) {
      setMsg($("tgReconnectMsg"), e.message || String(e), "err");
    }
  });
}

function closeAccountDrawer() {
  const el = $("accountDrawer");
  if (!el) return;
  el.hidden = true;
  document.body.classList.remove("drawer-open");
}

function fillAccountPanel(r) {
  const st = r.telegram || {};
  const u = st.user || {};
  if (st.authorized) {
    $("accName").textContent = u.first_name || "已登录";
    $("accMeta").textContent = u.username ? `@${u.username}` : "Telegram 会话有效 · 重启可自动重连";
  } else {
    $("accName").textContent = "未登录 Telegram";
    $("accMeta").textContent = r.session_exists
      ? "检测到会话文件，可点「重新连接」或重新登录"
      : "请在下方完成登录后即可下载";
  }
  $("accId").textContent = u.id != null ? String(u.id) : "—";
  $("accUsername").textContent = u.username ? `@${u.username}` : "—";
  $("accPhone").textContent = u.phone ? `+${u.phone}` : "—";
  $("accConnected").textContent = st.connected ? "已连接" : "未连接";
  if ($("accApiStatus")) {
    $("accApiStatus").textContent = r.api_configured
      ? `已配置${r.api_id ? ` · ID ${r.api_id}` : ""}`
      : "未配置";
  }
  if ($("accSessionStatus")) {
    $("accSessionStatus").textContent = r.session_exists ? "已保存（可自动重连）" : "无";
  }
  $("accDownloadDir").textContent = r.download_dir || "—";
  const proxyVal = r.proxy || st.proxy || "";
  if ($("accProxy")) $("accProxy").value = proxyVal || $("accProxy").value || "";
  if ($("proxy") && proxyVal) $("proxy").value = proxyVal;
  if (r.api_id && $("apiId") && !$("apiId").value) $("apiId").value = String(r.api_id);
  const pill = $("accStatusPill");
  pill.textContent = st.authorized ? "已登录" : "未登录";
  pill.className = "status-pill " + (st.authorized ? "status-completed" : "status-paused");
  applyAccountChip(st);
  setMsg($("accProxyMsg"), "");
  setMsg($("tgAuthMsg"), "");
  setMsg($("tgReconnectMsg"), "");
  fillWebUsersPanel(r);
}

function fillWebUsersPanel(r) {
  state.webUsername = r.web_username || null;
  state.webUsers = r.web_users || [];
  const cur = $("webCurrentUser");
  if (cur) cur.textContent = state.webUsername || "（未启用）";
  if ($("webProfileName")) $("webProfileName").textContent = state.webUsername || "未登录";
  if ($("webAvatarLg")) {
    $("webAvatarLg").textContent = state.webUsername
      ? initials(state.webUsername)
      : "Web";
  }

  const list = $("webUsersList");
  const sel = $("webPwdUsername");
  if (!list || !sel) return;

  if (!state.webUsers.length) {
    list.innerHTML = `<p class="hint">暂无 Web 账号（将使用 .env 中的 WEB_USERNAME / WEB_PASSWORD 自动创建）</p>`;
  } else {
    list.innerHTML = state.webUsers
      .map((u) => {
        const isSelf = state.webUsername && u.username === state.webUsername;
        const delBtn = isSelf && state.webUsers.length <= 1
          ? ""
          : `<button type="button" class="ghost danger-text" data-del-user="${escapeHtml(u.username)}">删除</button>`;
        return `<div class="web-user-row">
          <div>
            <strong>${escapeHtml(u.username)}</strong>
            ${isSelf ? '<span class="web-user-badge">当前</span>' : ""}
          </div>
          ${delBtn}
        </div>`;
      })
      .join("");
    list.querySelectorAll("[data-del-user]").forEach((btn) => {
      btn.addEventListener("click", () => deleteWebUser(btn.dataset.delUser));
    });
  }

  const names = state.webUsers.map((u) => u.username);
  const preferred = state.webUsername && names.includes(state.webUsername)
    ? state.webUsername
    : names[0] || "";
  sel.innerHTML = names
    .map(
      (n) =>
        `<option value="${escapeHtml(n)}" ${n === preferred ? "selected" : ""}>${escapeHtml(n)}</option>`
    )
    .join("");
  setMsg($("webPwdMsg"), "");
  setMsg($("webAddMsg"), "");
}

async function refreshWebUsers() {
  const r = await api("/api/auth/account");
  state.account = r;
  fillWebUsersPanel(r);
}

async function changeWebPassword() {
  const username = ($("webPwdUsername") && $("webPwdUsername").value) || "";
  const old_password = $("webPwdOld").value;
  const new_password = $("webPwdNew").value;
  const confirm_password = $("webPwdConfirm").value;
  if (!username) {
    setMsg($("webPwdMsg"), "请选择账号", "err");
    return;
  }
  if (!old_password) {
    setMsg($("webPwdMsg"), "请输入当前登录账号的密码以确认", "err");
    return;
  }
  if (!new_password || new_password.length < 4) {
    setMsg($("webPwdMsg"), "新密码至少 4 位", "err");
    return;
  }
  if (new_password !== confirm_password) {
    setMsg($("webPwdMsg"), "两次输入的新密码不一致", "err");
    return;
  }
  setMsg($("webPwdMsg"), "保存中…");
  try {
    const r = await api("/api/auth/web-users/change-password", {
      method: "POST",
      body: JSON.stringify({ username, old_password, new_password, confirm_password }),
    });
    if (!r.ok) {
      setMsg($("webPwdMsg"), r.message || "修改失败", "err");
      return;
    }
    $("webPwdOld").value = "";
    $("webPwdNew").value = "";
    $("webPwdConfirm").value = "";
    setMsg($("webPwdMsg"), `已更新 ${r.username} 的密码`, "ok");
  } catch (e) {
    setMsg($("webPwdMsg"), e.message || String(e), "err");
  }
}

async function addWebUser() {
  const username = $("webNewUsername").value.trim();
  const password = $("webNewPassword").value;
  if (!username) {
    setMsg($("webAddMsg"), "请输入用户名", "err");
    return;
  }
  if (!password || password.length < 4) {
    setMsg($("webAddMsg"), "密码至少 4 位", "err");
    return;
  }
  setMsg($("webAddMsg"), "创建中…");
  try {
    const r = await api("/api/auth/web-users", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    });
    if (!r.ok) {
      setMsg($("webAddMsg"), r.message || "创建失败", "err");
      return;
    }
    $("webNewUsername").value = "";
    $("webNewPassword").value = "";
    setMsg($("webAddMsg"), `已创建账号 ${r.user.username}`, "ok");
    await refreshWebUsers();
  } catch (e) {
    setMsg($("webAddMsg"), e.message || String(e), "err");
  }
}

async function deleteWebUser(username) {
  if (!username) return;
  const ok = await confirmDialog({
    title: "删除 Web 账号",
    message: `确认删除 Web 账号「${username}」？`,
    confirmText: "删除",
    danger: true,
  });
  if (!ok) return;
  try {
    const r = await api(`/api/auth/web-users/${encodeURIComponent(username)}`, {
      method: "DELETE",
    });
    if (!r.ok) {
      toast(r.message || "删除失败", "err");
      return;
    }
    toast(`已删除账号 ${username}`, "ok");
    if (state.webUsername && username === state.webUsername) {
      await logoutWeb(true);
      return;
    }
    await refreshWebUsers();
  } catch (e) {
    toast(e.message || String(e), "err");
  }
}

async function testConnection() {
  await withBusy($("btnTestConn"), async () => {
    setMsg($("tgAuthMsg"), "测试连接中…");
    try {
      const proxy = $("proxy").value.trim();
      await api("/api/auth/set-proxy", { method: "POST", body: JSON.stringify({ proxy }) });
      const r = await api("/api/auth/test-connection", { method: "POST", body: "{}" });
      if (r.ok) setMsg($("tgAuthMsg"), r.message || "连接成功", "ok");
      else setMsg($("tgAuthMsg"), r.message || "连接失败", "err");
    } catch (e) {
      setMsg($("tgAuthMsg"), e.message || String(e), "err");
    }
  });
}

async function sendCode() {
  const body = {
    phone: $("phone").value.trim(),
    api_id: Number($("apiId").value || 0) || null,
    api_hash: $("apiHash").value.trim() || null,
    proxy: $("proxy").value.trim() || null,
  };
  if (!body.phone) {
    setMsg($("tgAuthMsg"), "请填写手机号", "err");
    return;
  }
  await withBusy([$("btnSendCode"), $("btnTestConn")], async () => {
    setMsg($("tgAuthMsg"), "发送中…");
    try {
      const r = await api("/api/auth/send-code", { method: "POST", body: JSON.stringify(body) });
      if (r.status === "already_authorized" || r.authorized) {
        setMsg($("tgAuthMsg"), "已登录", "ok");
        await afterTgLogin();
        return;
      }
      if (r.ok === false) {
        setMsg($("tgAuthMsg"), r.message || "发送失败", "err");
        return;
      }
      setMsg($("tgAuthMsg"), "验证码已发送", "ok");
      setTgSub("code");
    } catch (e) {
      setMsg($("tgAuthMsg"), e.message || String(e), "err");
    }
  });
}

async function signIn() {
  const body = {
    code: $("code").value.trim(),
    password: $("tfa").value || null,
  };
  if (!body.code) {
    setMsg($("tgAuthMsg"), "请填写验证码", "err");
    return;
  }
  await withBusy($("btnSignIn"), async () => {
    setMsg($("tgAuthMsg"), "登录中…");
    try {
      const r = await api("/api/auth/sign-in", { method: "POST", body: JSON.stringify(body) });
      if (!r.ok) {
        setMsg($("tgAuthMsg"), r.message || "登录失败", "err");
        return;
      }
      setMsg($("tgAuthMsg"), "Telegram 登录成功", "ok");
      await afterTgLogin();
    } catch (e) {
      setMsg($("tgAuthMsg"), e.message || String(e), "err");
    }
  });
}

async function afterTgLogin() {
  await refreshTgState();
  const loginBlock = $("tgSettingsLogin");
  if (loginBlock) loginBlock.hidden = true;
  try {
    await loadChats();
  } catch (_) {}
  // refresh account panel summary
  try {
    const r = await api("/api/auth/account");
    fillAccountPanel(r);
    if ($("tgSettingsLogin")) $("tgSettingsLogin").hidden = true;
  } catch (_) {}
}

async function testAccountProxy() {
  await withBusy($("btnAccTestProxy"), async () => {
    setMsg($("accProxyMsg"), "测试中…");
    try {
      const proxy = $("accProxy").value.trim();
      await api("/api/auth/set-proxy", { method: "POST", body: JSON.stringify({ proxy }) });
      if ($("proxy")) $("proxy").value = proxy;
      const r = await api("/api/auth/test-connection", { method: "POST", body: "{}" });
      if (r.ok) setMsg($("accProxyMsg"), r.message || "连接成功", "ok");
      else setMsg($("accProxyMsg"), r.message || "连接失败", "err");
    } catch (e) {
      setMsg($("accProxyMsg"), e.message || String(e), "err");
    }
  });
}

async function saveAccountProxy() {
  await withBusy($("btnAccSaveProxy"), async () => {
    setMsg($("accProxyMsg"), "保存中…");
    try {
      const proxy = $("accProxy").value.trim();
      await api("/api/auth/set-proxy", { method: "POST", body: JSON.stringify({ proxy }) });
      if ($("proxy")) $("proxy").value = proxy;
      setMsg($("accProxyMsg"), "代理已保存（重启后仍生效）", "ok");
      toast("代理已保存", "ok");
    } catch (e) {
      setMsg($("accProxyMsg"), e.message || String(e), "err");
    }
  });
}

async function logoutTelegram() {
  const ok = await confirmDialog({
    title: "退出 Telegram",
    message: "确认退出 Telegram 账号？退出后需重新登录才能下载。",
    confirmText: "退出",
    danger: true,
  });
  if (!ok) return;
  await api("/api/auth/logout", { method: "POST", body: "{}" });
  state.tgAuthorized = false;
  setTgBanner(true);
  applyAccountChip({ authorized: false });
  if ($("tgSettingsLogin")) $("tgSettingsLogin").hidden = false;
  setTgSub("api");
  setMsg($("tgAuthMsg"), "已退出 Telegram，可重新登录", "ok");
  fillAccountPanel({
    telegram: { authorized: false, connected: false, user: null },
    proxy: $("accProxy").value,
    download_dir: $("accDownloadDir").textContent,
  });
}

async function logoutWeb(force = false) {
  if (!force) {
    const ok = await confirmDialog({
      title: "退出 Web 控制台",
      message: "退出后需重新输入 Web 密码。不会退出 Telegram。",
      confirmText: "退出控制台",
    });
    if (!ok) return;
  }
  await api("/api/auth/web-logout", { method: "POST", body: "{}" });
  closeAccountDrawer();
  stopPolling();
  try {
    sessionStorage.removeItem(TASKS_HTML_CACHE_KEY);
  } catch (_) {}
  showWebLogin();
  setMsg($("webAuthMsg"), "已退出控制台", "ok");
}

function selectChat(chat) {
  const id = chat.id ?? chat.chat_id;
  const item = {
    id,
    title: chat.title || String(chat.chat_id || chat.id),
    username: chat.username || "",
    kind: chat.kind || "group",
  };
  const idx = state.selectedChats.findIndex((c) => String(c.id) === String(id));
  if (idx >= 0) {
    state.selectedChats.splice(idx, 1);
  } else {
    state.selectedChats.push(item);
  }
  updateSelectedUi();
  renderChats($("chatSearch").value);
  if (normalizeDownloadMode($("downloadMode") && $("downloadMode").value) === "monitor") {
    refreshIndexPanel();
  }
}

function updateSelectedUi() {
  const el = $("selectedChat");
  const box = $("selectedBox");
  const hint = $("selectedCountHint");
  const n = state.selectedChats.length;
  if (!el) return;
  if (!n) {
    el.textContent = "未选择群组";
    el.classList.remove("has-selection");
    if (box) box.classList.remove("active");
    if (hint) hint.textContent = "点选群组可多选，将为每个群各建一个任务";
    return;
  }
  if (n === 1) {
    el.textContent = state.selectedChats[0].title;
  } else {
    const names = state.selectedChats.slice(0, 3).map((c) => c.title).join("、");
    el.textContent = n <= 3 ? names : `${names} 等 ${n} 个群`;
  }
  el.classList.add("has-selection");
  if (box) box.classList.add("active");
  if (hint) {
    hint.textContent =
      n === 1
        ? "已选 1 个群组；继续点选可批量创建"
        : `已选 ${n} 个群组，将批量创建 ${n} 个任务`;
  }
}

function expandChatDropdown() {
  const body = $("chatDropdownBody");
  if (!body) return;
  body.classList.remove("collapsed");
  const btn = $("btnToggleChats");
  if (btn) btn.setAttribute("aria-expanded", "true");
}

async function loadChats(opts = {}) {
  if (!state.tgAuthorized) {
    $("chatList").innerHTML = `<div class="chat-item"><span class="meta">请先在设置中登录 Telegram</span></div>`;
    return;
  }
  if (state._chatsInflight && !opts.force) return state._chatsInflight;
  const run = (async () => {
    const listEl = $("chatList");
    if (listEl && !state.chats.length) {
      listEl.innerHTML = `<div class="chat-item list-loading"><span class="meta">正在加载群组…</span></div>`;
    }
    try {
      const q = $("chatSearch").value.trim();
      const r = await api(`/api/chats?q=${encodeURIComponent(q)}`);
      if (!r.ok) {
        if (listEl && !state.chats.length) {
          listEl.innerHTML = `<div class="chat-item"><span>${escapeHtml(r.message || "加载失败")}</span></div>`;
        } else if (opts.force) {
          toast(r.message || "群组刷新失败", "err");
        }
        return;
      }
      state.chats = r.chats || [];
      renderChats(q);
    } catch (e) {
      if (isSoftAuthError(e)) return;
      // Keep previous list on network blip
      if (listEl && !state.chats.length) {
        listEl.innerHTML = `<div class="chat-item"><span>${escapeHtml(e.message || "加载失败")}</span></div>`;
      } else if (opts.force) {
        toast(e.message || "群组刷新失败", "err");
      }
    }
  })();
  state._chatsInflight = run.finally(() => {
    state._chatsInflight = null;
  });
  return state._chatsInflight;
}

function renderChats(filter = "") {
  const q = (filter || "").toLowerCase();
  const list = state.chats.filter((c) => {
    if (!q) return true;
    return (c.title || "").toLowerCase().includes(q) || (c.username || "").toLowerCase().includes(q);
  });
  if (!list.length) {
    $("chatList").innerHTML = `<div class="chat-item"><span class="meta">没有匹配的群组</span></div>`;
    return;
  }
  $("chatList").innerHTML = list
    .map((c) => {
      const active = state.selectedChats.some((s) => String(s.id) === String(c.id)) ? "active" : "";
      const uname = c.username ? `@${c.username}` : c.kind;
      return `<div class="chat-item ${active}" data-id="${c.id}">
        <div>
          <div class="title">${escapeHtml(c.title)}</div>
          <div class="meta">${escapeHtml(uname)} · id ${c.id}</div>
        </div>
      </div>`;
    })
    .join("");

  $("chatList").querySelectorAll(".chat-item").forEach((el) => {
    el.addEventListener("click", () => {
      const id = el.dataset.id;
      const chat = state.chats.find((c) => String(c.id) === String(id));
      if (chat) selectChat(chat);
    });
  });
}

async function createTask() {
  if (!state.tgAuthorized) {
    setMsg($("taskMsg"), "请先在设置中登录 Telegram", "err");
    closeCreateModal();
    openSettings(true);
    return;
  }
  if (!state.selectedChats.length) {
    setMsg($("taskMsg"), "请先选择至少一个群组", "err");
    return;
  }
  const opts = buildTaskOptions();
  if (opts.download_mode === "monitor") {
    // 监控模式 = 建索引任务：默认媒体类型，标签稍后在设置里配
    if (!opts.media_types.length) {
      opts.media_types = ["photo", "video", "document", "audio"];
    }
    opts.include_tags = [];
    opts.caption_keywords = [];
  } else if (!opts.media_types.length) {
    setMsg($("taskMsg"), "请至少勾选一种媒体类型", "err");
    return;
  }
  await withBusy([$("btnCreateTask"), $("btnCancelCreate")], async () => {
    setMsg($("taskMsg"), state.selectedChats.length > 1 ? "批量创建中…" : "创建中…");
    try {
      let okMsg = "任务已创建并开始";
      if (state.selectedChats.length === 1) {
        const c = state.selectedChats[0];
        const r = await api("/api/tasks", {
          method: "POST",
          body: JSON.stringify({ ...opts, chat_id: c.id, chat_title: c.title }),
        });
        if (!r.ok) {
          setMsg($("taskMsg"), r.message || "创建失败", "err");
          return;
        }
      } else {
        const r = await api("/api/tasks/batch", {
          method: "POST",
          body: JSON.stringify({
            ...opts,
            chats: state.selectedChats.map((c) => ({ chat_id: c.id, chat_title: c.title })),
          }),
        });
        if (!r.ok) {
          setMsg($("taskMsg"), r.message || "批量创建失败", "err");
          return;
        }
        okMsg = `已创建 ${r.count || 0} 个任务并开始`;
      }
      closeCreateModal();
      switchPage("tasks");
      toast(okMsg, "ok");
      await loadTasks({ force: true });
    } catch (e) {
      setMsg($("taskMsg"), e.message || String(e), "err");
    }
  });
}

async function loadHistory(offset = 0) {
  const listEl = $("historyList");
  if (!listEl) return;
  if (state._historyInflight) return state._historyInflight;
  const run = (async () => {
    state.historyOffset = Math.max(0, offset);
    const q = ($("historySearch") && $("historySearch").value.trim()) || "";
    const btnPrev = $("btnHistoryPrev");
    const btnNext = $("btnHistoryNext");
    const btnRefresh = $("btnRefreshHistory");
    const busyBtns = [btnPrev, btnNext, btnRefresh].filter(Boolean);
    busyBtns.forEach((b) => {
      b.disabled = true;
      b.classList.add("is-busy");
    });
    if (!listEl.querySelector(".history-item")) {
      listEl.innerHTML = `<div class="empty-tasks list-loading">加载中…</div>`;
    }
    try {
      const r = await api(
        `/api/history?q=${encodeURIComponent(q)}&status=done&limit=${state.historyLimit}&offset=${state.historyOffset}`
      );
      state.historyTotal = r.total || 0;
      const items = r.items || [];
      if (!items.length) {
        listEl.innerHTML = `<div class="empty-tasks">暂无下载记录<br/><span class="muted">${q ? "换个关键词试试" : "下载完成后会出现在这里"}</span></div>`;
      } else {
        listEl.innerHTML = items
          .map((it) => {
            const name = it.file_name || (it.file_path || "").split(/[/\\]/).pop() || "—";
            const when = (it.created_at || "").replace("T", " ").slice(0, 19);
            const kind = it.media_kind || "file";
            const previewable = it.previewable ? "1" : "0";
            const thumb =
              kind === "image" && it.previewable
                ? `<div class="history-thumb"><img src="/api/history/${it.id}/file" alt="" loading="lazy" /></div>`
                : kind === "video"
                  ? `<div class="history-thumb history-thumb-icon" aria-hidden="true"><span class="ui-ico ui-ico-video"></span></div>`
                  : kind === "audio"
                    ? `<div class="history-thumb history-thumb-icon" aria-hidden="true"><span class="ui-ico ui-ico-audio"></span></div>`
                    : `<div class="history-thumb history-thumb-icon" aria-hidden="true"><span class="ui-ico ui-ico-file"></span></div>`;
            const meta = `${it.chat_title || it.chat_id || ""} · msg ${it.message_id} · 任务 #${it.task_id}${it.previewable ? " · 点击预览" : ""}`;
            return `<div class="history-item" role="button" tabindex="0" data-id="${escapeHtml(String(it.id))}" data-kind="${escapeHtml(kind)}" data-previewable="${previewable}" data-name="${escapeHtml(name)}">
          ${thumb}
          <div class="history-item-main">
            <div class="history-name" title="${escapeHtml(it.file_path || name)}">${escapeHtml(name)}</div>
            <div class="history-meta" title="${escapeHtml(meta)}">${escapeHtml(meta)}</div>
            <div class="history-time">${escapeHtml(when)}</div>
          </div>
        </div>`;
          })
          .join("");
      }
      const info = $("historyPageInfo");
      if (info) {
        const from = state.historyTotal ? state.historyOffset + 1 : 0;
        const to = Math.min(state.historyOffset + state.historyLimit, state.historyTotal);
        info.textContent = state.historyTotal ? `${from}–${to} / ${state.historyTotal}` : "0";
      }
      if (btnPrev) btnPrev.disabled = state.historyOffset <= 0;
      if (btnNext) {
        btnNext.disabled = state.historyOffset + state.historyLimit >= state.historyTotal;
      }
      if (btnRefresh) btnRefresh.disabled = false;
    } catch (e) {
      listEl.innerHTML = `<div class="empty-tasks">加载失败<br/><span class="muted">${escapeHtml(e.message || String(e))}</span></div>`;
      if (btnPrev) btnPrev.disabled = state.historyOffset <= 0;
      if (btnNext) btnNext.disabled = true;
      if (btnRefresh) btnRefresh.disabled = false;
      throw e;
    } finally {
      busyBtns.forEach((b) => b.classList.remove("is-busy"));
    }
  })();
  state._historyInflight = run.finally(() => {
    state._historyInflight = null;
  });
  return state._historyInflight;
}

function openHistoryPreview(id, kind, name) {
  const modal = $("historyPreviewModal");
  const body = $("historyPreviewBody");
  const title = $("historyPreviewTitle");
  const meta = $("historyPreviewMeta");
  const openLink = $("historyPreviewOpen");
  if (!modal || !body) return;
  const url = `/api/history/${id}/file`;
  if (title) title.textContent = name || "媒体预览";
  if (meta) meta.textContent = kind || "";
  if (openLink) openLink.href = url;
  body.innerHTML = "";
  if (kind === "image") {
    body.innerHTML = `<img class="history-preview-media" src="${url}" alt="${escapeHtml(name || "")}" />`;
  } else if (kind === "video") {
    body.innerHTML = `<video class="history-preview-media" src="${url}" controls playsinline></video>`;
  } else if (kind === "audio") {
    body.innerHTML = `<audio class="history-preview-media history-preview-audio" src="${url}" controls></audio>`;
  } else {
    body.innerHTML = `<p class="muted">无法内嵌预览，请用「新窗口打开」。</p>`;
  }
  modal.hidden = false;
  syncBodyModalLock();
}

function closeHistoryPreview() {
  const modal = $("historyPreviewModal");
  const body = $("historyPreviewBody");
  if (body) {
    body.querySelectorAll("video, audio").forEach((el) => {
      try {
        el.pause();
        el.removeAttribute("src");
        el.load();
      } catch (_) {}
    });
    body.innerHTML = "";
  }
  if (modal) modal.hidden = true;
  syncBodyModalLock();
}

function captureTaskLogScroll() {
  const map = {};
  const list = $("taskList");
  if (!list) return map;
  list.querySelectorAll(".task").forEach((taskEl) => {
    const id =
      taskEl.querySelector("[data-id]")?.dataset?.id ||
      taskEl.dataset.taskId;
    const body = taskEl.querySelector(".task-log-body");
    if (!id || !body) return;
    map[id] = {
      top: body.scrollTop,
      pinTop: body.scrollTop < 12,
      height: body.scrollHeight,
    };
  });
  return map;
}

function restoreTaskLogScroll(map) {
  if (!map || !Object.keys(map).length) return;
  const list = $("taskList");
  if (!list) return;
  list.querySelectorAll(".task").forEach((taskEl) => {
    const id =
      taskEl.querySelector("[data-id]")?.dataset?.id ||
      taskEl.dataset.taskId;
    const body = taskEl.querySelector(".task-log-body");
    const saved = id && map[id];
    if (!body || !saved) return;
    if (saved.pinTop) {
      body.scrollTop = 0; // newest-on-top: stay pinned to latest
      return;
    }
    // keep same viewport when new lines prepend (height may grow)
    const delta = Math.max(0, body.scrollHeight - (saved.height || 0));
    body.scrollTop = saved.top + delta;
  });
}

function bindTaskActions(root) {
  const list = root || $("taskList");
  if (!list) return;
  // Event delegation — survives cache restore / innerHTML patch / soft boot
  if (list.dataset.actionsBound === "1") return;
  list.dataset.actionsBound = "1";
  list.addEventListener("click", async (ev) => {
    const btn = ev.target.closest("[data-action]");
    if (!btn || !list.contains(btn) || btn.disabled) return;
    const id = btn.dataset.id;
    const action = btn.dataset.action;
    if (!id || !action) return;
    ev.preventDefault();
    ev.stopPropagation();

    if (action === "open-settings") {
      openTaskTagsModal(id);
      return;
    }
    if (action === "show-queue" || action === "show-matches" || action === "show-done") {
      const kind =
        action === "show-matches" ? "matches" : action === "show-done" ? "done" : "queue";
      openTaskFilesModal(id, kind).catch((e) => {
        if (!isSoftAuthError(e)) toast(e.message || String(e), "err");
      });
      return;
    }

    const busyKey = `${action}:${id}`;
    if (!state._taskActionBusy) state._taskActionBusy = new Set();
    if (state._taskActionBusy.has(busyKey)) return;
    state._taskActionBusy.add(busyKey);
    btn.disabled = true;
    btn.classList.add("is-busy");
    try {
      if (action === "start") {
        toast("正在启动任务…", "info", 2000);
        const r = await api(`/api/tasks/${id}/start`, { method: "POST", body: "{}" });
        if (!r.ok) throw new Error(r.message || "启动失败");
        toast("任务已继续", "ok", 2000);
      } else if (action === "pause") {
        toast("正在暂停…", "info", 2000);
        await api(`/api/tasks/${id}/pause`, { method: "POST", body: "{}" });
      } else if (action === "delete") {
        const ok = await confirmDialog({
          title: "删除任务",
          message: "确认删除该任务记录？已下载文件不会删除。",
          confirmText: "删除任务",
          danger: true,
        });
        if (!ok) return;
        await api(`/api/tasks/${id}`, { method: "DELETE" });
      } else if (action === "clear-log") {
        const ok = await confirmDialog({
          title: "清空活动日志",
          message: "确认清空该任务的活动日志？不影响下载进度与已下载文件。",
          confirmText: "清空日志",
          danger: true,
        });
        if (!ok) return;
        await api(`/api/tasks/${id}/clear-log`, { method: "POST", body: "{}" });
        toast("日志已清空", "ok", 1800);
      }
      await loadTasks({ force: true });
    } catch (e) {
      if (!isSoftAuthError(e)) {
        toast("操作失败: " + (e.message || e), "err");
        console.error(e);
      }
    } finally {
      state._taskActionBusy.delete(busyKey);
      btn.classList.remove("is-busy");
      // Disabled state follows next patchTaskCard / render
      const task = (state.tasks || []).find((t) => String(t.id) === String(id));
      if (action === "start") {
        btn.disabled = task ? task.status === "running" : false;
      } else if (action === "pause") {
        btn.disabled = task ? task.status !== "running" : true;
      } else {
        btn.disabled = false;
      }
    }
  });
}

function stepNumberInput(input, dir) {
  if (!input) return;
  const step = Number(input.step) || 1;
  const min = input.min === "" ? -Infinity : Number(input.min);
  const max = input.max === "" ? Infinity : Number(input.max);
  let cur = Number(input.value);
  if (!Number.isFinite(cur)) cur = Number.isFinite(min) && min > -Infinity ? min : 0;
  const next = dir === "down" ? cur - step : cur + step;
  const clamped = Math.min(max, Math.max(min, next));
  // Keep one decimal for 0.1 steps, otherwise integer-ish
  const decimals = String(input.step || "1").includes(".") ? 1 : 0;
  input.value = decimals ? clamped.toFixed(decimals) : String(Math.round(clamped));
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function closeTaskTagsModal() {
  closeTagPicker();
  const modal = $("tagsModal");
  if (modal) modal.hidden = true;
  document.body.classList.remove("confirm-open");
  state.taskTagsDraft = null;
  state.tagPickerIndex = [];
  state.tagPickerBundles = [];
  state.tagPickerRelated = {};
  // Stop settings-driven index polling when leaving the modal
  if (!($("createModal") && !$("createModal").hidden)) {
    stopIndexPolling();
  }
}

function groupKey(tags) {
  return (tags || [])
    .map((t) => String(t || "").toLowerCase())
    .filter(Boolean)
    .sort()
    .join("\0");
}

function syncDraftTagsFromGroups() {
  const draft = state.taskTagsDraft;
  if (!draft) return;
  const seen = new Set();
  const flat = [];
  for (const g of draft.groups || []) {
    for (const raw of g || []) {
      const tag = normalizeTagName(raw);
      if (!tag) continue;
      const key = tag.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      flat.push(tag);
    }
  }
  draft.tags = flat;
}

/**
 * Relation blacklist: never join/bridge co-occurrence clusters.
 * Loaded from /api/settings/tag-blacklist (editable in 设置).
 */
let TAG_RELATION_BLACKLIST = new Set();

function applyTagBlacklist(tags) {
  const list = (tags || [])
    .map((t) => String(t || "").trim().replace(/^#/, ""))
    .filter(Boolean);
  state.tagBlacklist = list;
  TAG_RELATION_BLACKLIST = new Set(list.map((t) => t.toLowerCase()));
}

async function loadTagBlacklist(opts = {}) {
  const render = opts.render !== false;
  try {
    const r = await api("/api/settings/tag-blacklist");
    applyTagBlacklist(r.tags || []);
    state.tagBlacklistDefaults = r.defaults || [];
    if (render) renderBlacklistPanel();
    return r;
  } catch (e) {
    if (render) setMsg($("blTagMsg"), e.message || String(e), "err");
    throw e;
  }
}

function renderBlacklistPanel() {
  const list = $("blTagList");
  const countEl = $("blTagCount");
  if (countEl) countEl.textContent = `${state.tagBlacklist.length} 个`;
  if (!list) return;
  if (!state.tagBlacklist.length) {
    list.innerHTML = "";
    return;
  }
  list.innerHTML = state.tagBlacklist
    .map(
      (t) => `<span class="bl-chip" title="#${escapeHtml(t)}">
        <span>#${escapeHtml(t)}</span>
        <button type="button" data-bl-del="${escapeHtml(t)}" aria-label="删除 #${escapeHtml(t)}">×</button>
      </span>`
    )
    .join("");
}

async function addBlacklistTag() {
  const input = $("blTagInput");
  const raw = (input && input.value) || "";
  const name = String(raw).trim().replace(/^#/, "");
  if (!name) {
    setMsg($("blTagMsg"), "请输入标签名", "err");
    return;
  }
  await withBusy($("btnBlAdd"), async () => {
    try {
      const r = await api("/api/settings/tag-blacklist", {
        method: "POST",
        body: JSON.stringify({ tag: name }),
      });
      applyTagBlacklist(r.tags || []);
      renderBlacklistPanel();
      if (input) input.value = "";
      setMsg($("blTagMsg"), `已添加 #${name}`, "ok");
      toast(`已加入黑名单 #${name}`, "ok");
    } catch (e) {
      setMsg($("blTagMsg"), e.message || String(e), "err");
    }
  });
}

async function removeBlacklistTag(tag) {
  const name = String(tag || "").trim().replace(/^#/, "");
  if (!name) return;
  try {
    const r = await api(`/api/settings/tag-blacklist/${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
    applyTagBlacklist(r.tags || []);
    renderBlacklistPanel();
    setMsg($("blTagMsg"), `已移除 #${name}`, "ok");
  } catch (e) {
    setMsg($("blTagMsg"), e.message || String(e), "err");
  }
}

async function resetBlacklistTags() {
  const ok = await confirmDialog({
    title: "恢复默认黑名单",
    message: "将覆盖当前列表为出厂默认项，确定吗？",
    confirmText: "恢复默认",
    danger: true,
  });
  if (!ok) return;
  await withBusy($("btnBlReset"), async () => {
    try {
      const r = await api("/api/settings/tag-blacklist/reset", {
        method: "POST",
        body: "{}",
      });
      applyTagBlacklist(r.tags || []);
      renderBlacklistPanel();
      setMsg($("blTagMsg"), `已恢复默认（${r.count || 0} 个）`, "ok");
      toast("黑名单已恢复默认", "ok");
    } catch (e) {
      setMsg($("blTagMsg"), e.message || String(e), "err");
    }
  });
}

function isBlacklistedTag(name) {
  const t = normalizeTagName(name);
  return !!(t && TAG_RELATION_BLACKLIST.has(t.toLowerCase()));
}

function stripBlacklistedTags(tags) {
  return (tags || [])
    .map((t) => normalizeTagName(t))
    .filter((t) => t && !isBlacklistedTag(t));
}

/**
 * Merge same-caption bundles via shared tags (indirect related).
 * Blacklisted + high-degree hub tags do NOT bridge others.
 */
/**
 * UI clustering for tag picker (display only).
 * Download / folder merge use backend Union-Find (organizer.TagUnionFind):
 * same-caption co-occurrence + blacklist strip, no hubDegree.
 * This function adds hubDegree so ultra-common tags don't glue the whole picker.
 */
function clusterRelatedBundles(bundles, opts = {}) {
  const hubDegree = Math.max(3, Number(opts.hubDegree) || 6);
  const maxCluster = Math.max(4, Number(opts.maxClusterSize) || 16);
  const raw = [];
  for (const b of bundles || []) {
    // Drop blacklist first so #半糖 never enters any 关联 group
    const tags = stripBlacklistedTags(b.tags || []);
    if (tags.length < 2) continue;
    // dedupe
    const seen = new Set();
    const uniq = [];
    for (const t of tags) {
      const k = t.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      uniq.push(t);
    }
    if (uniq.length < 2) continue;
    raw.push({ tags: uniq, count: Number(b.count) || 0 });
  }
  const degree = new Map();
  for (const b of raw) {
    for (const t of b.tags) {
      const k = t.toLowerCase();
      degree.set(k, (degree.get(k) || 0) + 1);
    }
  }
  const isHub = (t) =>
    isBlacklistedTag(t) ||
    (degree.get(String(t).toLowerCase()) || 0) >= hubDegree;

  const parent = new Map();
  const find = (k) => {
    if (!parent.has(k)) parent.set(k, k);
    let cur = k;
    while (parent.get(cur) !== cur) {
      parent.set(cur, parent.get(parent.get(cur)));
      cur = parent.get(cur);
    }
    return cur;
  };
  const union = (a, b) => {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) parent.set(rb, ra);
  };

  // Only non-hub tags may bridge; hubs stay attached per-bundle later
  for (const b of raw) {
    const bridges = b.tags.filter((t) => !isHub(t));
    if (bridges.length >= 2) {
      for (let i = 1; i < bridges.length; i++) {
        union(bridges[0].toLowerCase(), bridges[i].toLowerCase());
      }
    }
  }

  // component(root) -> Set of non-hub keys
  const bridgeComps = new Map();
  for (const k of parent.keys()) {
    const r = find(k);
    if (!bridgeComps.has(r)) bridgeComps.set(r, new Set());
    bridgeComps.get(r).add(k);
  }

  // Attach each raw bundle onto a cluster:
  // - if bundle has >=1 non-hub: join that bridge component (+ hubs from this bundle)
  // - if bundle is only hubs / hub+nothing useful: keep as its own pattern row
  const clusterMap = new Map(); // id -> { tags: Map lower->display, count }
  const ensureCluster = (id) => {
    if (!clusterMap.has(id)) {
      clusterMap.set(id, { tagMap: new Map(), count: 0 });
    }
    return clusterMap.get(id);
  };
  const addTo = (id, tags, count) => {
    const c = ensureCluster(id);
    for (const t of tags) c.tagMap.set(t.toLowerCase(), t);
    c.count += count;
  };

  let singletonSeq = 0;
  for (const b of raw) {
    const bridges = b.tags.filter((t) => !isHub(t));
    if (bridges.length >= 1) {
      const id = `c:${find(bridges[0].toLowerCase())}`;
      addTo(id, b.tags, b.count);
    } else {
      // all hubs — keep pattern separate
      addTo(`s:${singletonSeq++}`, b.tags, b.count);
    }
  }

  const clusters = [];
  for (const c of clusterMap.values()) {
    const tags = [...c.tagMap.values()];
    if (tags.length < 2) continue;
    if (tags.length > maxCluster) {
      // explode oversized clusters back to raw patterns involving these tags
      const keys = new Set(tags.map((t) => t.toLowerCase()));
      for (const b of raw) {
        if (!b.tags.some((t) => keys.has(t.toLowerCase()))) continue;
        // only emit if this bundle isn't dominated by a single mega-hub bridging
        clusters.push({ tags: b.tags.slice(), count: b.count });
      }
      continue;
    }
    clusters.push({ tags, count: c.count });
  }

  // Dedupe identical tag-sets (from explode)
  const seenKey = new Set();
  const out = [];
  for (const c of clusters) {
    const key = groupKey(c.tags);
    if (seenKey.has(key)) continue;
    seenKey.add(key);
    out.push(c);
  }
  out.sort((a, b) => b.count - a.count);
  return { clusters: out, isHub, degree };
}

function rebuildDraftGroupsFromTags(tags, bundles) {
  const list = (tags || []).map(normalizeTagName).filter(Boolean);
  const remaining = new Set(list.map((t) => t.toLowerCase()));
  const byLower = new Map(list.map((t) => [t.toLowerCase(), t]));
  const groups = [];
  const { clusters } = clusterRelatedBundles(bundles);
  for (const c of clusters) {
    const members = (c.tags || []).map((t) => normalizeTagName(t)).filter(Boolean);
    if (members.length < 2) continue;
    if (!members.every((m) => remaining.has(m.toLowerCase()))) continue;
    groups.push(members.map((m) => byLower.get(m.toLowerCase()) || m));
    members.forEach((m) => remaining.delete(m.toLowerCase()));
  }
  for (const t of list) {
    if (!remaining.has(t.toLowerCase())) continue;
    groups.push([t]);
    remaining.delete(t.toLowerCase());
  }
  return groups;
}

function ensureDraftGroups() {
  const draft = state.taskTagsDraft;
  if (!draft) return;
  if (!Array.isArray(draft.groups)) {
    draft.groups = rebuildDraftGroupsFromTags(
      draft.tags || [],
      state.tagPickerBundles || []
    );
  }
  syncDraftTagsFromGroups();
}

function renderTagsModalSummary() {
  const el = $("tagsModalSummary");
  const draft = state.taskTagsDraft;
  if (!el || !draft) return;
  ensureDraftGroups();
  const groups = draft.groups || [];
  if (!groups.length) {
    el.textContent = "点击选择标签";
    el.classList.add("muted");
    el.classList.remove("has-tags");
    el.title = "";
    return;
  }
  const parts = groups.map((g) => g.map((t) => "#" + t).join("+"));
  const preview = parts.join(" · ");
  el.textContent =
    parts.length > 3
      ? `${parts.slice(0, 3).join(" · ")} 等 ${parts.length} 组`
      : preview;
  el.title = preview;
  el.classList.remove("muted");
  el.classList.add("has-tags");
}

function fillTaskSettingsFields(task) {
  const tags = Array.isArray(task.include_tags) ? task.include_tags.slice() : [];
  state.taskTagsDraft = {
    taskId: String(task.id),
    chatId: task.chat_id,
    tags,
    groups: tags.map((t) => [normalizeTagName(t)]).filter((g) => g[0]),
    mode: "any",
    title: task.chat_title || String(task.chat_id),
    concurrency: Math.max(1, Math.min(5, Number(task.concurrency) || 2)),
    delayMin: Number(task.delay_min != null ? task.delay_min : 0.5),
    delayMax: Number(task.delay_max != null ? task.delay_max : 0.5),
  };
  if ($("tagsModalTitle")) $("tagsModalTitle").textContent = "任务设置";
  if ($("tagsModalSub")) $("tagsModalSub").textContent = state.taskTagsDraft.title;
  if ($("tagsModalRelated")) $("tagsModalRelated").checked = true;
  if ($("tagsModalConcurrency")) {
    $("tagsModalConcurrency").value = String(state.taskTagsDraft.concurrency);
  }
  if ($("tagsModalDelayMin")) {
    $("tagsModalDelayMin").value = String(state.taskTagsDraft.delayMin);
  }
  if ($("tagsModalDelayMax")) {
    $("tagsModalDelayMax").value = String(state.taskTagsDraft.delayMax);
  }
  renderTagsModalSummary();
}

function isTagApplied(name) {
  const key = String(name || "").toLowerCase();
  return (state.taskTagsDraft?.tags || []).some((t) => t.toLowerCase() === key);
}

function isDraftGroupApplied(names) {
  const draft = state.taskTagsDraft;
  if (!draft) return false;
  ensureDraftGroups();
  const key = groupKey(names);
  if (!key) return false;
  return (draft.groups || []).some((g) => groupKey(g) === key);
}

/** Expand seed to full related cluster (blacklist / hubs never glue everyone). */
function resolveRelatedTagGroup(name) {
  const tag = normalizeTagName(name);
  if (!tag) return [];
  // Blacklisted seed: never auto-expand partners
  if (isBlacklistedTag(tag)) return [tag];

  const key = tag.toLowerCase();
  const { clusters, isHub } = clusterRelatedBundles(state.tagPickerBundles || []);

  // Hub seed: only take the strongest single pattern (avoid adding dozens of partners)
  if (isHub(tag)) {
    let best = null;
    for (const b of state.tagPickerBundles || []) {
      const members = stripBlacklistedTags(b.tags || []);
      if (members.length < 2) continue;
      if (!members.some((m) => m.toLowerCase() === key)) continue;
      const count = Number(b.count) || 0;
      if (!best || count > best.count) best = members;
    }
    if (best?.length) {
      const rest = best.filter(
        (t) => t.toLowerCase() !== key && !isBlacklistedTag(t)
      );
      return [tag, ...rest];
    }
    return [tag];
  }

  const hit = clusters.find((c) =>
    (c.tags || []).some((t) => String(t).toLowerCase() === key)
  );
  if (hit?.tags?.length) {
    const rest = stripBlacklistedTags(hit.tags).filter(
      (t) => t.toLowerCase() !== key
    );
    return [tag, ...rest];
  }
  return [tag];
}

function addDraftTag(name) {
  const draft = state.taskTagsDraft;
  const tag = normalizeTagName(name);
  if (!draft || !tag) return false;
  ensureDraftGroups();
  const group = resolveRelatedTagGroup(tag);
  if (!group.length) return false;
  if (group.every((t) => isTagApplied(t))) {
    toast(
      group.length > 1
        ? `#${tag} 及相关标签已在列表中`
        : `#${tag} 已在列表中`,
      "err"
    );
    return false;
  }
  const drop = new Set(group.map((t) => t.toLowerCase()));
  draft.groups = (draft.groups || [])
    .map((g) => g.filter((t) => !drop.has(String(t).toLowerCase())))
    .filter((g) => g.length);
  draft.groups.push(group);
  syncDraftTagsFromGroups();
  renderTagsModalSummary();
  renderTagPickerApplied();
  renderTagPickerIndex();
  if (group.length > 1) {
    toast(`已添加关联组 ${group.map((t) => "#" + t).join(" · ")}`, "ok");
  }
  return true;
}

function removeDraftGroup(idx) {
  const draft = state.taskTagsDraft;
  if (!draft) return;
  ensureDraftGroups();
  const next = (draft.groups || []).slice();
  if (idx < 0 || idx >= next.length) return;
  next.splice(idx, 1);
  draft.groups = next;
  syncDraftTagsFromGroups();
  renderTagsModalSummary();
  renderTagPickerApplied();
  renderTagPickerIndex();
}

function removeDraftTag(idx) {
  // legacy: treat as group index
  removeDraftGroup(idx);
}

function removeDraftTagByName(name) {
  const draft = state.taskTagsDraft;
  if (!draft) return;
  ensureDraftGroups();
  const key = String(name || "").toLowerCase();
  draft.groups = (draft.groups || [])
    .map((g) => g.filter((t) => String(t).toLowerCase() !== key))
    .filter((g) => g.length);
  syncDraftTagsFromGroups();
  renderTagsModalSummary();
  renderTagPickerApplied();
  renderTagPickerIndex();
}

function renderTagPickerApplied() {
  const host = $("tagPickerApplied");
  const draft = state.taskTagsDraft;
  if (!host || !draft) return;
  ensureDraftGroups();
  const groups = draft.groups || [];
  syncTagPickerTabCounts();
  if (!groups.length) {
    host.innerHTML = `<div class="tag-picker-empty">尚未选择标签<br/><span class="muted">切换到「库内索引」点选，或在下方手动添加</span>
      <button type="button" class="primary tag-picker-goto-index" id="btnGotoIndexPage">去选标签</button>
    </div>`;
    $("btnGotoIndexPage")?.addEventListener("click", () => switchTagPickerPage("index"));
    return;
  }
  const phone = isPhoneTagPicker();
  host.innerHTML = groups
    .map((group, idx) => {
      const isBundle = group.length > 1;
      const chips = group
        .map(
          (tag) =>
            `<span class="tag-chip${isBundle ? " is-in-bundle" : " is-solo-applied"}">#${escapeHtml(
              tag
            )}</span>`
        )
        .join(
          isBundle && !phone
            ? `<span class="tag-chip-link" aria-hidden="true">+</span>`
            : ""
        );
      if (phone) {
        return `<div class="tag-picker-row is-applied${
          isBundle ? " tag-picker-bundle" : " tag-picker-solo"
        }" data-gidx="${idx}">
          <div class="tag-card-head">
            <span class="tag-bundle-mark">${
              isBundle ? `${group.length} 关联` : "单标签"
            }</span>
            <button type="button" class="tag-remove" data-gidx="${idx}" title="移除" aria-label="移除">×</button>
          </div>
          <div class="tag-chip-row">${chips}</div>
        </div>`;
      }
      return `<div class="tag-picker-row is-applied${
        isBundle ? " tag-picker-bundle" : " tag-picker-solo"
      }" data-gidx="${idx}">
        <div class="tag-chip-row">${
          isBundle
            ? `<span class="tag-bundle-mark">${group.length} 关联</span>`
            : ""
        }${chips}</div>
        <button type="button" class="tag-remove" data-gidx="${idx}" title="移除整组" aria-label="移除">×</button>
      </div>`;
    })
    .join("");
  host.querySelectorAll(".tag-remove").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      removeDraftGroup(Number(btn.dataset.gidx));
    });
  });
}

/** Rows for picker: related clusters first, then solo tags. */
function buildTagPickerRows(items, bundles) {
  const countBy = new Map();
  for (const t of items || []) {
    const name = String(t.tag || "").trim();
    if (!name) continue;
    countBy.set(name.toLowerCase(), {
      tag: name,
      count: Number(t.count) || 0,
    });
  }
  const { clusters } = clusterRelatedBundles(bundles);
  const inBundle = new Set();
  const rows = [];
  for (const c of clusters) {
    // Extra guard: never show blacklisted tags inside 关联 cards
    const names = stripBlacklistedTags(c.tags || []);
    if (names.length < 2) continue;
    const members = names.map((name) => {
      const hit = countBy.get(name.toLowerCase());
      return { tag: hit?.tag || name, count: hit?.count || 0 };
    });
    members.forEach((m) => inBundle.add(m.tag.toLowerCase()));
    // sort members by count so hub-looking tags don't always lead
    members.sort(
      (a, b) => b.count - a.count || a.tag.localeCompare(b.tag, "zh")
    );
    rows.push({
      kind: "bundle",
      tags: members,
      count: Number(c.count) || 0,
    });
  }
  rows.sort((a, b) => b.count - a.count);
  const solos = [];
  for (const item of countBy.values()) {
    if (inBundle.has(item.tag.toLowerCase())) continue;
    solos.push({ kind: "solo", tags: [item], count: item.count });
  }
  solos.sort(
    (a, b) => b.count - a.count || a.tags[0].tag.localeCompare(b.tags[0].tag, "zh")
  );
  return rows.concat(solos);
}

function renderTagPickerIndex() {
  const host = $("tagPickerIndex");
  if (!host) return;
  const q = (($("tagPickerSearch") && $("tagPickerSearch").value) || "")
    .trim()
    .replace(/^#+/, "")
    .toLowerCase();
  // Cap bundles fed into clustering — keeps mobile main thread responsive
  const allBundles = state.tagPickerBundles || [];
  const bundleCap = q ? 800 : 300;
  const rows = buildTagPickerRows(
    state.tagPickerIndex || [],
    allBundles.length > bundleCap ? allBundles.slice(0, bundleCap) : allBundles
  );
  let visible = rows;
  if (q) {
    visible = rows.filter((row) =>
      row.tags.some((t) => String(t.tag || "").toLowerCase().includes(q))
    );
  }
  if (!visible.length) {
    host.innerHTML = `<div class="tag-picker-empty">${
      (state.tagPickerIndex || []).length
        ? "没有匹配的索引标签"
        : "暂无索引标签<br/><span class=\"muted\">请先在任务设置里更新该群索引</span>"
    }</div>`;
    return;
  }
  // Limit DOM nodes; search to find the rest
  const rowCap = q ? 200 : 120;
  const truncated = visible.length > rowCap;
  const shownRows = truncated ? visible.slice(0, rowCap) : visible;
  host.innerHTML = shownRows
    .map((row, i) => {
      const group = [...row.tags].sort(
        (a, b) => b.count - a.count || a.tag.localeCompare(b.tag, "zh")
      );
      const isBundle = row.kind === "bundle";
      const names = group.map((t) => t.tag);
      const exactApplied = isBundle
        ? isDraftGroupApplied(names)
        : isTagApplied(names[0]);
      const allApplied = group.every((t) => isTagApplied(t.tag));
      const anyApplied = group.some((t) => isTagApplied(t.tag));
      // Phone: show every tag in the bundle (no +N truncation)
      const phone = isPhoneTagPicker();
      const maxShow = phone ? group.length : Math.min(group.length, 8);
      const shown = group.slice(0, maxShow);
      const more = group.length - shown.length;
      const chips = shown
        .map((t, idx) => {
          const hit = q && String(t.tag).toLowerCase().includes(q);
          return `<span class="tag-chip${idx === 0 && isBundle ? " is-primary" : ""}${
            hit ? " is-hit" : ""
          }">#${escapeHtml(t.tag)}</span>`;
        })
        .join(
          isBundle && !phone
            ? `<span class="tag-chip-link" aria-hidden="true">+</span>`
            : ""
        );
      const moreChip =
        !phone && more > 0
          ? `<span class="tag-chip is-more" title="${escapeHtml(
              group
                .slice(maxShow)
                .map((t) => "#" + t.tag)
                .join(" ")
            )}">+${more}</span>`
          : "";
      let meta;
      if (isBundle) {
        meta = phone
          ? `<span class="tag-meta-c">${row.count} 次</span>${
              exactApplied || allApplied
                ? `<span class="tag-meta-ok">已选</span>`
                : ""
            }`
          : `<span class="tag-meta-n">${group.length}</span><span class="tag-meta-sep">关联</span><span class="tag-meta-c">${row.count}</span>${
              exactApplied || allApplied
                ? `<span class="tag-meta-ok">已选</span>`
                : ""
            }`;
      } else {
        meta = exactApplied
          ? `<span class="tag-meta-ok">已选</span>`
          : `<span class="tag-meta-c">${phone ? row.count + " 次" : row.count}</span>`;
      }
      if (phone) {
        return `<button type="button" class="tag-picker-row${
          isBundle ? " tag-picker-bundle" : " tag-picker-solo"
        }${exactApplied || allApplied ? " is-applied" : anyApplied ? " is-partial" : ""}" data-gidx="${i}">
          <div class="tag-card-head">
            <span class="tag-bundle-mark">${
              isBundle ? `${group.length} 关联` : "单标签"
            }</span>
            <span class="tag-meta">${meta}</span>
          </div>
          <div class="tag-chip-row">${chips}</div>
        </button>`;
      }
      return `<button type="button" class="tag-picker-row${
        isBundle ? " tag-picker-bundle" : " tag-picker-solo"
      }${exactApplied || allApplied ? " is-applied" : anyApplied ? " is-partial" : ""}" data-gidx="${i}">
        <div class="tag-chip-row">${
          isBundle
            ? `<span class="tag-bundle-mark">${group.length} 关联</span>`
            : ""
        }${chips}${moreChip}</div>
        <span class="tag-meta">${meta}</span>
      </button>`;
    })
    .join("")
    + (truncated
      ? `<div class="tag-picker-empty" style="padding:10px">已显示前 ${rowCap} 条，共 ${visible.length} 条<br/><span class="muted">输入关键词可精确筛选</span></div>`
      : "");
  syncTagPickerTabCounts();
  host.querySelectorAll(".tag-picker-row[data-gidx]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = shownRows[Number(btn.dataset.gidx)];
      if (!row?.tags?.length) return;
      toggleDraftTagGroup(row.tags.map((t) => t.tag));
    });
  });
}

function toggleDraftTagGroup(names) {
  const draft = state.taskTagsDraft;
  if (!draft || !names?.length) return;
  const list = names.map(normalizeTagName).filter(Boolean);
  if (!list.length) return;
  ensureDraftGroups();
  const key = groupKey(list);
  const drop = new Set(list.map((t) => t.toLowerCase()));
  const exactIdx = (draft.groups || []).findIndex((g) => groupKey(g) === key);
  const allOn = list.every((t) => isTagApplied(t));

  if (allOn) {
    if (exactIdx >= 0) {
      draft.groups.splice(exactIdx, 1);
    } else {
      // remove these tags wherever they sit
      draft.groups = (draft.groups || [])
        .map((g) => g.filter((t) => !drop.has(String(t).toLowerCase())))
        .filter((g) => g.length);
    }
  } else {
    // pull tags out of other groups, then add as one whole row
    draft.groups = (draft.groups || [])
      .map((g) => g.filter((t) => !drop.has(String(t).toLowerCase())))
      .filter((g) => g.length);
    draft.groups.push(list);
  }
  syncDraftTagsFromGroups();
  renderTagsModalSummary();
  renderTagPickerApplied();
  renderTagPickerIndex();
}

function _yieldUi() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function isPhoneTagPicker() {
  try {
    return !!(
      window.matchMedia &&
      (window.matchMedia("(max-width: 820px)").matches ||
        window.matchMedia("(pointer: coarse)").matches)
    );
  } catch (_) {
    return false;
  }
}

function switchTagPickerPage(page) {
  const p = page === "applied" ? "applied" : "index";
  state.tagPickerPage = p;
  const modal = $("tagPickerModal");
  if (modal) {
    modal.dataset.pickerPage = p;
    modal.classList.toggle("picker-page-applied", p === "applied");
    modal.classList.toggle("picker-page-index", p === "index");
  }
  document.querySelectorAll(".tag-picker-tab[data-picker-page]").forEach((btn) => {
    const on = btn.dataset.pickerPage === p;
    btn.classList.toggle("is-active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
  });
  document.querySelectorAll(".tag-picker-page[data-picker-page]").forEach((el) => {
    el.classList.toggle("is-active-page", el.dataset.pickerPage === p);
  });
  if (p === "index") closeTagPickerSuggest();
  syncTagPickerTabCounts();
}

function syncTagPickerTabCounts() {
  const groups = state.taskTagsDraft?.groups || [];
  const appliedN = groups.length;
  const indexN = (state.tagPickerIndex || []).length;
  const a1 = $("tagPickerAppliedCount");
  const a2 = $("tagPickerTabAppliedCount");
  const i2 = $("tagPickerTabIndexCount");
  if (a1) a1.textContent = String(appliedN);
  if (a2) a2.textContent = String(appliedN);
  if (i2) i2.textContent = indexN ? String(indexN) : "…";
}

async function openTagPicker() {
  const draft = state.taskTagsDraft;
  if (!draft) {
    toast("请先打开任务设置", "err");
    return;
  }
  const modal = $("tagPickerModal");
  if (!modal) return;
  const tagsModal = $("tagsModal");
  tagsModal?.classList.add("under-picker");
  // Clear any stuck loading shield on settings panel
  tagsModal?.querySelector(".tags-panel")?.classList.remove("is-loading");

  modal.hidden = false;
  document.body.classList.add("confirm-open");
  if ($("tagPickerSearch")) $("tagPickerSearch").value = "";
  if ($("tagPickerInput")) $("tagPickerInput").value = "";
  closeTagPickerSuggest();
  // Default to index page (mobile tabs); desktop still shows both columns
  switchTagPickerPage("index");
  renderTagPickerApplied();
  const indexHost = $("tagPickerIndex");
  if (indexHost) {
    indexHost.innerHTML = `<div class="tag-picker-empty">加载索引标签…</div>`;
  }
  syncTagPickerTabCounts();

  let loadOk = true;
  const ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timer = ctrl ? setTimeout(() => ctrl.abort(), 20000) : null;
  try {
    const r = await api(`/api/index/${encodeURIComponent(draft.chatId)}/tags`, {
      signal: ctrl ? ctrl.signal : undefined,
    });
    state.tagPickerIndex = Array.isArray(r.tags) ? r.tags : [];
    state.tagPickerBundles = Array.isArray(r.bundles) ? r.bundles : [];
    state.tagPickerRelated = {};
  } catch (e) {
    loadOk = false;
    state.tagPickerIndex = [];
    state.tagPickerBundles = [];
    state.tagPickerRelated = {};
    const msg =
      e?.name === "AbortError"
        ? "加载超时，请缩小索引后重试"
        : e.message || "索引标签加载失败";
    toast(msg, "err");
    if (indexHost) {
      indexHost.innerHTML = `<div class="tag-picker-empty">加载失败<br/><span class="muted">${escapeHtml(
        msg
      )}</span></div>`;
    }
  } finally {
    if (timer) clearTimeout(timer);
  }

  try {
    await _yieldUi();
    if (draft.groups) {
      draft.groups = rebuildDraftGroupsFromTags(
        draft.tags || [],
        state.tagPickerBundles
      );
      syncDraftTagsFromGroups();
    }
    renderTagPickerApplied();
    await _yieldUi();
    if (loadOk) renderTagPickerIndex();
  } catch (e) {
    console.error("tag picker render failed", e);
    if (indexHost) {
      indexHost.innerHTML = `<div class="tag-picker-empty">渲染失败<br/><span class="muted">${escapeHtml(
        e.message || String(e)
      )}</span></div>`;
    }
  }
}

function closeTagPicker() {
  const modal = $("tagPickerModal");
  if (modal) modal.hidden = true;
  $("tagsModal")?.classList.remove("under-picker");
  // Keep body lock if task-settings modal is still open underneath
  if ($("tagsModal")?.hidden !== false) {
    document.body.classList.remove("confirm-open");
  }
  closeTagPickerSuggest();
  renderTagsModalSummary();
}

function closeTagPickerSuggest() {
  const menu = $("tagPickerSuggestMenu");
  if (menu) {
    menu.hidden = true;
    menu.innerHTML = "";
  }
}

function matchTagPickerSuggestions(query) {
  const q = normalizeTagName(query).toLowerCase();
  if (!q) return [];
  const applied = new Set(
    (state.taskTagsDraft?.tags || []).map((t) => String(t).toLowerCase())
  );
  const scored = [];
  for (const item of state.tagPickerIndex || []) {
    const name = String(item.tag || "");
    const key = name.toLowerCase();
    if (!key) continue;
    let score = -1;
    if (key === q) score = 300;
    else if (key.startsWith(q)) score = 200;
    else if (key.includes(q)) score = 100;
    if (score < 0) continue;
    if (applied.has(key)) score -= 40;
    scored.push({
      tag: name,
      count: item.count ?? 0,
      applied: applied.has(key),
      score: score + Math.min(20, Number(item.count) || 0) / 1000,
    });
  }
  scored.sort((a, b) => b.score - a.score || b.count - a.count);
  return scored.slice(0, 12);
}

function renderTagPickerSuggest() {
  const input = $("tagPickerInput");
  const menu = $("tagPickerSuggestMenu");
  if (!input || !menu) return;
  const items = matchTagPickerSuggestions(input.value);
  if (!items.length) {
    closeTagPickerSuggest();
    return;
  }
  menu.hidden = false;
  menu.innerHTML = items
    .map(
      (t, i) => `<button type="button" class="tag-picker-suggest-item${
        t.applied ? " is-applied" : ""
      }${i === 0 ? " is-active" : ""}" role="option" data-tag="${escapeHtml(t.tag)}">
        <span class="tag-name">#${escapeHtml(t.tag)}</span>
        <span class="tag-meta">${t.applied ? "已选" : escapeHtml(String(t.count))}</span>
      </button>`
    )
    .join("");
  menu.querySelectorAll(".tag-picker-suggest-item").forEach((btn) => {
    btn.addEventListener("mousedown", (e) => {
      e.preventDefault();
      pickTagFromSuggest(btn.dataset.tag);
    });
  });
}

function pickTagFromSuggest(name) {
  const tag = normalizeTagName(name);
  if (!tag) return;
  addDraftTag(tag);
  const input = $("tagPickerInput");
  if (input) input.value = "";
  closeTagPickerSuggest();
  input?.focus();
}

function addTagFromPickerInput() {
  const input = $("tagPickerInput");
  const menu = $("tagPickerSuggestMenu");
  const active =
    menu && !menu.hidden
      ? menu.querySelector(".tag-picker-suggest-item.is-active")
      : null;
  if (active?.dataset?.tag) {
    pickTagFromSuggest(active.dataset.tag);
    return;
  }
  if (addDraftTag(input && input.value) && input) {
    input.value = "";
    closeTagPickerSuggest();
  }
}

function syncTaskSettingsPicker(preferredId) {
  const wrap = $("tagsModalTaskWrap");
  const sel = $("tagsModalTaskSelect");
  const tasks = state.tasks || [];
  if (!wrap || !sel) return;
  if (tasks.length <= 1) {
    wrap.hidden = true;
    return;
  }
  wrap.hidden = false;
  const cur = preferredId != null ? String(preferredId) : String(sel.value || "");
  sel.innerHTML = tasks
    .map(
      (t) =>
        `<option value="${t.id}">${escapeHtml(t.chat_title || t.chat_id)} (#${t.id})</option>`
    )
    .join("");
  if (cur && [...sel.options].some((o) => o.value === cur)) sel.value = cur;
  else if (sel.options.length) sel.selectedIndex = 0;
}

async function openTaskTagsModal(taskId, opts = {}) {
  const modal = $("tagsModal");
  if (!modal) return;
  const panel = modal.querySelector(".tags-panel");
  panel?.classList.add("is-loading");
  if (!opts.keepOpen || modal.hidden) {
    modal.hidden = false;
    document.body.classList.add("confirm-open");
  }
  let task = null;
  try {
    const r = await api(`/api/tasks/${taskId}`);
    task = r.task;
  } catch (_) {
    task = null;
  } finally {
    panel?.classList.remove("is-loading");
  }
  if (!task) {
    toast("加载任务失败", "err");
    if (!opts.keepOpen) closeTaskTagsModal();
    return;
  }
  fillTaskSettingsFields(task);
  syncTaskSettingsPicker(task.id);
  // Load index status for this task's chat (scan buttons live here now)
  refreshIndexPanel().catch(() => {});
}

function parseKeywordsInput(raw) {
  return String(raw || "")
    .split(/[,，\s]+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

async function saveTaskTagsModal() {
  const draft = state.taskTagsDraft;
  if (!draft) return;
  const expand = $("tagsModalRelated")?.checked !== false;
  const concurrency = Math.max(
    1,
    Math.min(5, Number($("tagsModalConcurrency")?.value) || 2)
  );
  let delayMin = Number($("tagsModalDelayMin")?.value);
  let delayMax = Number($("tagsModalDelayMax")?.value);
  if (!Number.isFinite(delayMin) || delayMin < 0) delayMin = 0.5;
  if (!Number.isFinite(delayMax) || delayMax < 0) delayMax = delayMin;
  if (delayMax < delayMin) delayMax = delayMin;
  const autoEnabled = !!$("tagsModalAutoIndex")?.checked;
  const autoInterval = Math.max(
    5,
    Math.min(1440, Number($("tagsModalAutoIndexInterval")?.value) || 60)
  );

  const btn = $("btnTagsModalSave");
  if (btn) btn.disabled = true;
  try {
    const r = await api(`/api/tasks/${draft.taskId}/settings`, {
      method: "PATCH",
      body: JSON.stringify({
        include_tags: draft.tags || [],
        tag_match_mode: "any",
        expand_related: expand,
        concurrency,
        delay_min: delayMin,
        delay_max: delayMax,
      }),
    });
    if (!r.ok) throw new Error(r.message || "保存失败");
    // Persist timed incremental index (per chat)
    await api(`/api/index/${encodeURIComponent(draft.chatId)}/auto-scan`, {
      method: "PATCH",
      body: JSON.stringify({
        enabled: autoEnabled,
        interval_min: autoInterval,
        chat_title: draft.title || "",
      }),
    });
    const tagN = (draft.tags || []).length;
    toast(
      autoEnabled
        ? `任务设置已保存${tagN ? " · 开始按标签下载" : ""} · 自动增量每 ${autoInterval} 分钟`
        : `任务设置已保存${tagN ? " · 开始按标签下载" : ""}`,
      "ok"
    );
    closeTaskTagsModal();
    await loadTasks();
  } catch (e) {
    toast("保存失败: " + (e.message || e), "err");
  } finally {
    if (btn) btn.disabled = false;
  }
}

function liveProgressSignature(t) {
  const live = t.live;
  if (live && live.phase === "indexing") return "indexing";
  if (!live || (!live.file && !(live.files && live.files.length))) {
    return t.status === "running" ? "idle" : "none";
  }
  const files = Array.isArray(live.files) ? live.files : [];
  if (files.length > 1) return `multi:${files.map((f) => f.id).join(",")}`;
  return `one:${live.file || (files[0] && files[0].file) || ""}`;
}

function indexProgressPercent(live) {
  if (live && live.percent != null && !Number.isNaN(Number(live.percent))) {
    return Math.min(100, Math.max(0, Number(live.percent)));
  }
  const indexedLast = Number(live?.indexed_last) || 0;
  const chatLatest = Number(live?.chat_latest) || 0;
  if (chatLatest > 0) {
    return Math.min(100, Math.max(0, Math.round((1000 * indexedLast) / chatLatest) / 10));
  }
  return 0;
}

function patchDownloadProgressBox(box, t) {
  const live = t.live || {};
  const files = Array.isArray(live.files) ? live.files : [];
  const speedText = formatSpeed(live.speed, { waiting: t.status === "running" });
  if (box.classList.contains("multi") && files.length > 1) {
    const title = box.querySelector(".live-summary-title");
    const speed = box.querySelector(".live-summary-meta .live-speed");
    const active = live.active_count || files.length || 1;
    if (title) title.textContent = `并发下载中 · ${active} 路`;
    if (speed) speed.textContent = `合计 ${speedText}`;
    const host = box.querySelector(".live-files");
    if (host) host.innerHTML = files.map(renderFileProgressRow).join("");
    return true;
  }
  const name = live.file || (files[0] && files[0].file) || "";
  const fileEl = box.querySelector(".live-file");
  const metaSpans = box.querySelectorAll(".live-meta > span");
  const fill = box.querySelector(".prog-fill");
  const track = box.querySelector(".prog-track");
  const pct = live.percent != null ? live.percent : null;
  const sizeLine = live.total
    ? `${formatBytes(live.received)} / ${formatBytes(live.total)}${pct != null ? ` (${pct}%)` : ""}`
    : live.received
      ? formatBytes(live.received)
      : "准备中…";
  if (fileEl) {
    fileEl.textContent = `正在下载：${name}`;
    fileEl.title = name;
  }
  if (metaSpans[0]) metaSpans[0].textContent = sizeLine;
  if (metaSpans[1]) metaSpans[1].textContent = speedText;
  if (track && fill) {
    if (pct != null) {
      track.classList.remove("indeterminate");
      fill.style.width = `${Math.min(100, pct)}%`;
    } else {
      track.classList.add("indeterminate");
      fill.style.width = "";
    }
  }
  return true;
}

function patchIndexProgressBox(box, t) {
  const live = t.live || {};
  const scanned = Number(live.scanned) || 0;
  const media = Number(live.media) || 0;
  const title = live.title || live.file || "文案索引处理中…";
  const pct = indexProgressPercent(live);
  const detail =
    live.detail || `已看 ${scanned} 条消息 · 已存 ${media} 条媒体 · 总进度 ${pct}%`;
  const indexedLast = Number(live.indexed_last) || 0;
  const chatLatest = Number(live.chat_latest) || 0;
  const behind = Number(live.behind) || 0;
  const titleEl = box.querySelector('[data-role="index-title"]');
  const detailEl = box.querySelector('[data-role="index-detail"]');
  const rangeEl = box.querySelector('[data-role="index-range"]');
  const errEl = box.querySelector('[data-role="index-error"]');
  const pctEl = box.querySelector('[data-role="index-pct"]');
  const fillEl = box.querySelector(".prog-fill");
  if (titleEl) titleEl.textContent = title;
  if (detailEl) detailEl.textContent = detail;
  if (pctEl) pctEl.textContent = `${pct}%`;
  if (fillEl) fillEl.style.width = `${pct}%`;
  if (rangeEl) {
    if (chatLatest || indexedLast) {
      rangeEl.hidden = false;
      rangeEl.textContent = `索引至 ${indexedLast || "—"}${
        chatLatest ? ` / 群最新 ${chatLatest}` : ""
      }${behind > 0 ? ` · 落后 ${behind}` : ""} · ${pct}%`;
    } else {
      rangeEl.hidden = true;
    }
  }
  if (errEl) {
    if (live.error) {
      errEl.hidden = false;
      errEl.textContent = String(live.error);
    } else {
      errEl.hidden = true;
      errEl.textContent = "";
    }
  }
}

function patchTaskCard(el, t) {
  const statusClass = `status-${t.status || "pending"}`;
  const pill = el.querySelector(".status-pill");
  if (pill) {
    pill.className = `status-pill ${statusClass}`;
    pill.textContent = statusLabel(t.status, t);
  }
  const queueCount = Math.max(
    0,
    Number(t.tag_match_count ?? 0) - Number(t.tag_processed_count ?? 0)
  );
  const statsBox = el.querySelector(".task-stats");
  if (statsBox && !statsBox.querySelector('[data-role="queue-stat"]')) {
    statsBox.innerHTML = `
      <button type="button" class="task-stat-btn" data-role="match-stat" data-action="show-matches" data-id="${escapeHtml(String(t.id))}" title="查看标签命中">
        <span class="ui-ico ui-ico-tag" aria-hidden="true"></span>
        <strong data-role="match-count">${t.tag_match_count ?? 0}</strong>
        <span class="task-stat-label">命中</span>
      </button>
      <button type="button" class="task-stat-btn" data-role="done-stat" data-action="show-done" data-id="${escapeHtml(String(t.id))}" title="查看已处理">
        <span class="ui-ico ui-ico-check" aria-hidden="true"></span>
        <strong data-role="done-count">${t.tag_processed_count ?? 0}</strong>
        <span class="task-stat-label">已处理</span>
      </button>
      <button type="button" class="task-stat-btn" data-role="queue-stat" data-action="show-queue" data-id="${escapeHtml(String(t.id))}" title="查看待下载队列">
        <span class="ui-ico ui-ico-queue" aria-hidden="true"></span>
        <strong data-role="queue-count">${queueCount}</strong>
        <span class="task-stat-label">队列</span>
      </button>`;
  } else {
    const mEl = el.querySelector('[data-role="match-count"]');
    const dEl = el.querySelector('[data-role="done-count"]');
    const qEl = el.querySelector('[data-role="queue-count"]');
    if (mEl) mEl.textContent = String(t.tag_match_count ?? 0);
    if (dEl) dEl.textContent = String(t.tag_processed_count ?? 0);
    if (qEl) qEl.textContent = String(queueCount);
    el.querySelectorAll(".task-stat-btn").forEach((btn) => {
      btn.dataset.id = String(t.id);
    });
  }
  const btnStart = el.querySelector('[data-action="start"]');
  const btnPause = el.querySelector('[data-action="pause"]');
  if (btnStart) btnStart.disabled = t.status === "running";
  if (btnPause) btnPause.disabled = t.status !== "running";

  // legacy meta / tags summary — remove if still present
  el.querySelectorAll(".task-head .meta").forEach((n) => n.remove());
  el.querySelector(':scope > [data-role="task-tags-summary"]')?.remove();

  // error message
  let errEl = el.querySelector(":scope > .msg.err");
  if (t.last_error) {
    if (!errEl) {
      errEl = document.createElement("p");
      errEl.className = "msg err is-visible";
      errEl.innerHTML = `<span class="msg-icon" aria-hidden="true"></span><span class="msg-text"></span>`;
      const actions = el.querySelector(".task-actions");
      if (actions) actions.after(errEl);
      else el.prepend(errEl);
    }
    const text = errEl.querySelector(".msg-text");
    if (text) text.textContent = t.last_error;
  } else if (errEl) {
    errEl.remove();
  }

  // live progress — preserve indexing bar DOM to avoid animation flicker
  const sig = liveProgressSignature(t);
  let liveHost = el.querySelector(":scope > .live-progress");
  const prevSig = liveHost?.dataset?.sig || "";
  if (sig === "indexing" && liveHost && liveHost.dataset.phase === "indexing") {
    patchIndexProgressBox(liveHost, t);
    liveHost.dataset.sig = sig;
  } else if (sig !== prevSig || !liveHost) {
    const html = renderLiveProgress(t);
    if (!html) {
      liveHost?.remove();
    } else {
      const wrap = document.createElement("div");
      wrap.innerHTML = html;
      const next = wrap.firstElementChild;
      if (next) next.dataset.sig = sig;
      if (liveHost) liveHost.replaceWith(next);
      else {
        const log = el.querySelector(":scope > .task-log");
        if (log) log.before(next);
        else el.appendChild(next);
      }
    }
  } else if (liveHost && sig !== "indexing" && sig !== "none" && sig !== "idle") {
    // same download shape — patch numbers in place (no DOM replace / flicker)
    patchDownloadProgressBox(liveHost, t);
    liveHost.dataset.sig = sig;
  }

  // log — replace when content changes, or UI shell is outdated (e.g. missing 清空)
  const logEl = el.querySelector(":scope > .task-log");
  const logKey = String(t.last_log || "");
  const logShellStale = !!(logEl && !logEl.querySelector(".log-clear-btn"));
  if (logEl && (logEl.dataset.logKey !== logKey || logShellStale)) {
    const scrollTop = logEl.querySelector(".task-log-body")?.scrollTop || 0;
    const pinTop = scrollTop < 12;
    const prevH = logEl.querySelector(".task-log-body")?.scrollHeight || 0;
    const wrap = document.createElement("div");
    wrap.innerHTML = renderTaskLog(t.last_log, t.id);
    const nextLog = wrap.firstElementChild;
    if (nextLog) {
      nextLog.dataset.logKey = logKey;
      logEl.replaceWith(nextLog);
      const body = nextLog.querySelector(".task-log-body");
      if (body) {
        if (pinTop) body.scrollTop = 0;
        else body.scrollTop = scrollTop + Math.max(0, body.scrollHeight - prevH);
      }
    }
  } else if (logEl) {
    logEl.dataset.logKey = logKey;
  }
}

async function loadTasks(opts = {}) {
  if (state._tasksInflight) {
    state._tasksQueued = true;
    return state._tasksInflight;
  }
  const run = (async () => {
    const scrollMap = captureTaskLogScroll();
    const list = $("taskList");
    try {
      const r = await api("/api/tasks");
      const tasks = r.tasks || [];
      state.tasks = tasks;
      if (!list) return;
      if (!tasks.length) {
        list.innerHTML = `<div class="empty-tasks">暂无下载任务<br/><span class="muted">点击右上角「新建下载」开始</span></div>`;
        try {
          sessionStorage.removeItem(TASKS_HTML_CACHE_KEY);
        } catch (_) {}
        return;
      }

      const existing = [...list.querySelectorAll(".task[data-task-id]")];
      const sameLayout =
        existing.length === tasks.length &&
        existing.every((el, i) => el.dataset.taskId === String(tasks[i].id));

      if (sameLayout) {
        // Yield between patches so the UI stays responsive with many tasks
        for (let i = 0; i < tasks.length; i++) {
          patchTaskCard(existing[i], tasks[i]);
          if (i > 0 && i % 8 === 0) await new Promise((r) => requestAnimationFrame(r));
        }
        restoreTaskLogScroll(scrollMap);
        bindTaskActions(list);
        // sessionStorage rewrite is expensive — only every ~8s while polling
        const now = Date.now();
        if (!state._tasksCacheAt || now - state._tasksCacheAt > 8000) {
          state._tasksCacheAt = now;
          cacheTaskListHtml(list.innerHTML);
        }
        return;
      }

      list.innerHTML = tasks.map(renderTask).join("");
      cacheTaskListHtml(list.innerHTML);
      list.querySelectorAll(".task-log").forEach((el, i) => {
        if (tasks[i]) el.dataset.logKey = String(tasks[i].last_log || "");
      });
      list.querySelectorAll(".live-progress").forEach((el) => {
        el.dataset.sig = el.dataset.phase || el.className;
      });
      restoreTaskLogScroll(scrollMap);
      bindTaskActions(list);
    } catch (e) {
      if (isSoftAuthError(e)) return;
      if (list && !list.querySelector(".task[data-task-id]")) {
        list.innerHTML = `<div class="empty-tasks">任务加载失败<br/><span class="muted">${escapeHtml(
          e.message || String(e)
        )}</span></div>`;
      }
      if (opts.force) throw e;
    }
  })();
  state._tasksInflight = run.finally(() => {
    state._tasksInflight = null;
    if (state._tasksQueued) {
      state._tasksQueued = false;
      loadTasks().catch(() => {});
    }
  });
  return state._tasksInflight;
}

function formatBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / (1024 * 1024)).toFixed(2) + " MB";
}

function formatSpeed(bps, opts = {}) {
  bps = Number(bps) || 0;
  if (bps <= 0) return opts.waiting ? "计算中…" : "—";
  if (bps < 1024) return bps.toFixed(0) + " B/s";
  if (bps < 1024 * 1024) return (bps / 1024).toFixed(1) + " KB/s";
  return (bps / (1024 * 1024)).toFixed(2) + " MB/s";
}

function renderFileProgressRow(f) {
  const pct = f.percent != null ? f.percent : null;
  const bar = pct != null
    ? `<div class="prog-track slim"><div class="prog-fill" style="width:${Math.min(100, pct)}%"></div></div>`
    : `<div class="prog-track slim indeterminate"><div class="prog-fill"></div></div>`;
  const sizeLine = f.total
    ? `${formatBytes(f.received)} / ${formatBytes(f.total)}`
    : (f.received ? formatBytes(f.received) : "…");
  const name = (f.file || "").split(/[/\\]/).pop() || f.file || `msg ${f.id}`;
  return `<div class="live-file-row">
    <div class="live-file-row-head">
      <span class="live-file-name" title="${escapeHtml(f.file || "")}">${escapeHtml(name)}</span>
      <span class="live-file-speed" title="该文件单独速度">${escapeHtml(
        formatSpeed(f.speed, { waiting: true })
      )}</span>
    </div>
    <div class="live-file-row-meta">${escapeHtml(sizeLine)}${pct != null ? ` · ${pct}%` : ""}</div>
    ${bar}
  </div>`;
}

function renderLiveProgress(t) {
  const live = t.live;
  if (live && live.phase === "indexing") {
    const scanned = Number(live.scanned) || 0;
    const media = Number(live.media) || 0;
    const title = live.title || live.file || "文案索引处理中…";
    const detail =
      live.detail ||
      `已看 ${scanned} 条消息 · 已存 ${media} 条媒体`;
    const indexedLast = Number(live.indexed_last) || 0;
    const chatLatest = Number(live.chat_latest) || 0;
    const behind = Number(live.behind) || 0;
    const pct = indexProgressPercent(live);
    const detailText =
      detail.includes("总进度") || !chatLatest
        ? detail
        : `${detail} · 总进度 ${pct}%`;
    return `<div class="live-progress indexing index-box" data-phase="indexing">
      <div class="index-box-kicker">索引进度</div>
      <div class="live-file" data-role="index-title">${escapeHtml(title)}</div>
      <div class="live-meta">
        <span data-role="index-detail">${escapeHtml(detailText)}</span>
        <span class="live-speed" data-role="index-pct">${pct}%</span>
      </div>
      <div class="index-box-range" data-role="index-range"${chatLatest || indexedLast ? "" : " hidden"}>索引至 ${indexedLast || "—"}${
        chatLatest ? ` / 群最新 ${chatLatest}` : ""
      }${behind > 0 ? ` · 落后 ${behind}` : ""} · ${pct}%</div>
      <div class="index-box-error" data-role="index-error"${live.error ? "" : " hidden"}>${escapeHtml(live.error || "")}</div>
      <div class="prog-track index-prog"><div class="prog-fill" style="width:${pct}%"></div></div>
    </div>`;
  }
  if (!live || (!live.file && !(live.files && live.files.length))) {
    if (t.status === "running") {
      return `<div class="live-progress idle">正在查找下一条媒体…</div>`;
    }
    return "";
  }
  const files = Array.isArray(live.files) ? live.files : [];
  const speedText = formatSpeed(live.speed, { waiting: t.status === "running" });
  const active = live.active_count || files.length || 1;

  // Multi-file: show per-file speed + summed total
  if (files.length > 1) {
    return `<div class="live-progress multi">
      <div class="live-summary">
        <div class="live-summary-title">并发下载中 · ${active} 路</div>
        <div class="live-summary-meta">
          <span class="live-speed">合计 ${escapeHtml(speedText)}</span>
        </div>
      </div>
      <div class="live-files">${files.map(renderFileProgressRow).join("")}</div>
    </div>`;
  }

  const pct = live.percent != null ? live.percent : null;
  const bar = pct != null
    ? `<div class="prog-track"><div class="prog-fill" style="width:${Math.min(100, pct)}%"></div></div>`
    : `<div class="prog-track indeterminate"><div class="prog-fill"></div></div>`;
  const sizeLine = live.total
    ? `${formatBytes(live.received)} / ${formatBytes(live.total)}${pct != null ? ` (${pct}%)` : ""}`
    : (live.received ? formatBytes(live.received) : "准备中…");
  const name = live.file || (files[0] && files[0].file) || "";
  return `<div class="live-progress">
    <div class="live-file" title="${escapeHtml(name)}">正在下载：${escapeHtml(name)}</div>
    <div class="live-meta"><span>${escapeHtml(sizeLine)}</span><span class="live-speed">${escapeHtml(speedText)}</span></div>
    ${bar}
  </div>`;
}

function parseLogLine(line) {
  const m = String(line || "").match(/^\[(\d{1,2}:\d{2}:\d{2})\]\s*(.*)$/);
  if (m) return { time: m[1], text: m[2] };
  return { time: "", text: String(line || "") };
}

function classifyLogText(text) {
  const t = String(text || "");
  if (/失败|错误|未登录|异常|不符|空文件|size mismatch|incomplete/i.test(t)) return "err";
  if (/已下载|任务完成|成功|已合并|已重新连接|登录成功|已下完|跳过/.test(t)) return "ok";
  if (/暂停|测试|限流|FloodWait|等待|上限|重试/.test(t)) return "warn";
  if (
    /正在下载|开始下载|启动|继续|优先重试|索引扫描|文案索引|建索引|断点续传|下载标签/.test(
      t
    )
  ) {
    return "busy";
  }
  return "info";
}

const LOG_KIND_LABEL = {
  err: "错误",
  ok: "完成",
  warn: "注意",
  busy: "进行中",
  info: "信息",
};

function humanizeLogText(text) {
  let t = String(text || "").trim();
  // Legacy / verbose lines → one clear sentence
  t = t.replace(
    /^开始标签\s+(#[^\s：:]+)[：:]\s*(\d+)\s*条(?:（[^）]*）)?$/,
    "下载标签 $1 · $2 条"
  );
  t = t.replace(
    /^下载标签\s+(#[^\s·]+)[：:]\s*(\d+)\s*条(?:（[^）]*）)?$/,
    "下载标签 $1 · $2 条"
  );
  t = t.replace(
    /^索引命中\s+(\d+)\s*条历史媒体，按标签逐个下完再下一个（(\d+)\s*个标签）$/,
    "索引命中 $1 条 · 按 $2 个标签依次下载"
  );
  t = t.replace(
    /^索引命中\s+(\d+)\s*条历史媒体，先补下再进入监控$/,
    "索引命中 $1 条 · 先补历史再监控"
  );
  t = t.replace(/^标签\s+(#[^\s]+)\s*已下完$/, "标签 $1 已下完，进入下一标签");
  t = t.replace(/^群组目录:\s*/, "保存目录 ");
  t = t.replace(/^目录:\s*/, "进入目录 ");
  t = t.replace(/^同任务并发:\s*/, "并发 ");
  t = t.replace(/^目录模式:\s*/, "目录模式 ");
  t = t.replace(/^任务模式:\s*/, "任务模式 ");
  t = t.replace(/^下载方向:\s*/, "下载方向 ");
  t = t.replace(/^最多下载:\s*/, "数量上限 ");
  t = t.replace(/^扩展名过滤:\s*/, "扩展名 ");
  t = t.replace(/^标签过滤[:：]?\s*/, "监控标签 ");
  t = t.replace(/^文案关键词:\s*/, "关键词 ");
  t = t.replace(/^随机延迟:\s*/, "下载延迟 ");
  t = t.replace(
    /^关联目录映射已启用（(\d+)\s*个标签）$/,
    "关联目录已启用 · $1 个标签"
  );
  t = t.replace(
    /^已扫描群目录，可按同名同大小跳过\s*(\d+)\s*个文件$/,
    "扫描群目录 · 可跳过 $1 个已存在文件"
  );
  t = t.replace(/已达上限\s*(\d+)\s*个文件，任务完成/, "已下满 $1 个文件，任务完成");
  t = t.replace(/测试模式时间到，已停止（未下完整文件）/, "测试模式结束（未下完整文件）");
  t = t.replace(/size mismatch:\s*got\s*(\d+),\s*expected\s*(\d+)/i, (_, a, b) => {
    const got = Number(a);
    const exp = Number(b);
    const gotMb = Number.isFinite(got) ? (got / (1024 * 1024)).toFixed(1) : a;
    const expMb = Number.isFinite(exp) ? (exp / (1024 * 1024)).toFixed(1) : b;
    return `大小不符 · 已下 ${gotMb} MB / 应为 ${expMb} MB`;
  });
  t = t.replace(/incomplete download:\s*(\d+)\/(\d+)/i, (_, a, b) => {
    const got = Number(a);
    const exp = Number(b);
    const gotMb = Number.isFinite(got) ? (got / (1024 * 1024)).toFixed(1) : a;
    const expMb = Number.isFinite(exp) ? (exp / (1024 * 1024)).toFixed(1) : b;
    return `下载不完整 · ${gotMb} MB / ${expMb} MB`;
  });
  t = t.replace(/下载重试\s*(\d+)\/(\d+)\s*\(msg\s*(\d+)\):\s*/i, "重试 $1/$2 · 消息 $3 · ");
  t = t.replace(/断点续传:\s*/, "断点续传 ");
  t = t.replace(/相同文件已存在，跳过:\s*/, "已存在，跳过 ");
  t = t.replace(/正在下载:\s*/g, "正在下载 ");
  t = t.replace(/已下载:\s*/g, "已下载 ");
  t = t.replace(/^已请求暂停：/, "已请求暂停 · ");
  t = t.replace(/^已暂停，保留进度:\s*/, "已暂停，保留进度 ");
  return t;
}

/** Split log into title / filename / path so the UI stays scannable. */
function structureLogText(text) {
  const raw = String(text || "").trim();
  const nice = humanizeLogText(raw);
  if (!nice) {
    return { title: "（无内容）", detail: "", sub: "", full: raw };
  }

  // File lines: title = action + size, detail = filename, sub = folder
  const fileVerb = nice.match(
    /^(正在下载|已下载|断点续传|已存在，跳过|测试占位|重试成功|已暂停，保留进度|暂停前已下完，保留|中断前已下完，保留)\s+(.+)$/
  );
  if (fileVerb) {
    const verb = fileVerb[1];
    const rest = fileVerb[2].trim();
    const sizeBit = rest.match(/（([^）]+)）\s*$/);
    const pathPart = sizeBit ? rest.slice(0, sizeBit.index).trim() : rest;
    const sizeHint = sizeBit ? sizeBit[1] : "";
    const norm = pathPart.replace(/\\/g, "/");
    const parts = norm.split("/").filter(Boolean);
    const file = parts.length ? parts[parts.length - 1] : pathPart;
    const folder = parts.length > 1 ? parts.slice(0, -1).join("/") : "";
    const title = sizeHint ? `${verb} · ${sizeHint}` : verb;
    return {
      title,
      detail: file,
      sub: folder,
      full: raw || nice,
    };
  }

  // Keep the whole sentence as title — do NOT split on "：" (it broke tag logs)
  return { title: nice, detail: "", sub: "", full: raw || nice };
}

function orderLogLinesNewestFirst(raw) {
  const lines = String(raw || "")
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);
  if (lines.length < 2) return lines;
  const times = lines
    .map((l) => {
      const m = l.match(/^\[(\d{2}:\d{2}:\d{2})\]/);
      return m ? m[1] : null;
    })
    .filter(Boolean);
  // Backend stores newest-first; only reverse legacy oldest-first blobs
  if (times.length >= 2 && times.join("|") === [...times].sort().join("|")) {
    return lines.slice().reverse();
  }
  return lines;
}

function renderTaskLog(raw, taskId) {
  const id = taskId != null ? String(taskId) : "";
  const clearBtn = id
    ? `<button type="button" class="ghost log-clear-btn" data-action="clear-log" data-id="${escapeHtml(id)}" title="清空日志">清空</button>`
    : "";
  const lines = orderLogLinesNewestFirst(raw);
  if (!lines.length) {
    return `<div class="task-log">
      <div class="task-log-head">
        <span>活动日志</span>
        <div class="task-log-head-right">
          <span class="muted">暂无</span>
          ${clearBtn}
        </div>
      </div>
      <div class="task-log-empty">暂无日志</div>
    </div>`;
  }
  const rows = lines
    .map((line) => {
      const { time, text } = parseLogLine(line);
      const kind = classifyLogText(text);
      const parts = structureLogText(text);
      const badge = LOG_KIND_LABEL[kind] || "信息";
      const detailHtml = parts.detail
        ? `<span class="log-detail">${escapeHtml(parts.detail)}</span>`
        : "";
      const subHtml = parts.sub
        ? `<span class="log-sub">${escapeHtml(parts.sub)}</span>`
        : "";
      const timeHtml = time
        ? `<span class="log-time">${escapeHtml(time)}</span>`
        : `<span class="log-time log-time-missing">无时间</span>`;
      return `<div class="log-line log-${kind}" title="${escapeHtml(parts.full || text)}">
        <div class="log-aside">
          ${timeHtml}
          <span class="log-badge">${badge}</span>
        </div>
        <div class="log-main">
          <span class="log-title">${escapeHtml(parts.title)}</span>
          ${detailHtml}
          ${subHtml}
        </div>
      </div>`;
    })
    .join("");
  return `<div class="task-log">
    <div class="task-log-head">
      <span>活动日志</span>
      <div class="task-log-head-right">
        <span class="muted">最新在上 · ${lines.length} 条</span>
        ${clearBtn}
      </div>
    </div>
    <div class="task-log-body">${rows}</div>
  </div>`;
}

function queueRemaining(t) {
  return Math.max(
    0,
    Number(t.tag_match_count ?? 0) - Number(t.tag_processed_count ?? 0)
  );
}

function _renderIndexFileRows(items) {
  return items
    .map((it) => {
      const tags = Array.isArray(it.tags) ? it.tags : [];
      const tagText = tags.length
        ? tags
            .slice(0, 6)
            .map((x) => `#${x}`)
            .join(" ")
        : "无标签";
      const cap = String(it.caption || "").replace(/\s+/g, " ").trim();
      const capShort = cap.length > 80 ? `${cap.slice(0, 80)}…` : cap;
      const when = (it.msg_date || it.created_at || "").replace("T", " ").slice(0, 19);
      const fileName = it.file_name || (it.file_path || "").split(/[/\\]/).pop() || "";
      const head = fileName
        ? fileName
        : `${it.media_type || "media"} · msg ${it.message_id}`;
      return `<div class="queue-item">
        <div class="queue-item-main">
          <div class="queue-name" title="${escapeHtml(it.file_path || head)}">${escapeHtml(head)}</div>
          <div class="queue-meta">msg ${escapeHtml(String(it.message_id))}${it.media_type ? ` · ${escapeHtml(it.media_type)}` : ""} · ${escapeHtml(tagText)}${when ? ` · ${escapeHtml(when)}` : ""}</div>
          ${capShort ? `<div class="queue-caption">${escapeHtml(capShort)}</div>` : ""}
        </div>
      </div>`;
    })
    .join("");
}

async function openTaskFilesModal(taskId, kind = "queue") {
  const modal = $("queueModal");
  const body = $("queueModalBody");
  const title = $("queueModalTitle");
  const sub = $("queueModalSub");
  if (!modal || !body) return;
  const kindN = kind === "matches" || kind === "done" ? kind : "queue";
  state._queueTaskId = String(taskId);
  state._filesKind = kindN;
  const labels = {
    matches: "标签命中",
    done: "已处理",
    queue: "队列",
  };
  const hadContent = !!body.querySelector(".queue-item, .queue-list");
  modal.hidden = false;
  syncBodyModalLock();
  if (title) title.textContent = labels[kindN];
  if (sub) sub.textContent = hadContent ? "刷新中…" : "加载中…";
  // Keep previous list while refreshing — avoid empty flash
  if (!hadContent) {
    body.innerHTML = `<div class="empty-tasks list-loading">加载中…</div>`;
  }
  let r;
  try {
    r = await api(`/api/tasks/${taskId}/files?kind=${kindN}&limit=80&offset=0`);
  } catch (e) {
    if (isSoftAuthError(e)) throw e;
    const msg = e.message || String(e);
    if (sub) sub.textContent = "加载失败";
    if (!hadContent) {
      body.innerHTML = `<div class="empty-tasks">加载失败<br/><span class="muted">${escapeHtml(msg)}<br/>若刚更新过，请重启服务后再试</span></div>`;
    } else {
      toast(msg, "err");
    }
    throw e;
  }
  if (title) {
    title.textContent = r.chat_title
      ? `${r.chat_title} · ${labels[kindN]}`
      : labels[kindN];
  }
  const active = r.active || [];
  const items = r.items || [];
  if (kindN === "queue") {
    if (sub) sub.textContent = `待下载 ${r.total || 0} · 正在下 ${r.active_count || 0}`;
  } else if (kindN === "matches") {
    if (sub) sub.textContent = `共 ${r.total || 0} 条匹配（显示 ${items.length}）`;
  } else {
    if (sub) sub.textContent = `共 ${r.total || 0} 个已完成（显示 ${items.length}）`;
  }
  if (!active.length && !items.length) {
    const emptyHint =
      kindN === "done"
        ? "还没有已下载完成的文件"
        : kindN === "matches"
          ? "索引里没有匹配的媒体"
          : "没有待下载的匹配媒体";
    body.innerHTML = `<div class="empty-tasks">暂无记录<br/><span class="muted">${emptyHint}</span></div>`;
    return;
  }
  let html = "";
  if (active.length) {
    html += `<div class="queue-section-title">正在下载 (${active.length})</div>`;
    html += `<div class="queue-list">${active
      .map((f) => {
        const pct = f.percent != null ? `${f.percent}%` : "…";
        const size =
          f.total > 0
            ? `${formatBytes(f.received)} / ${formatBytes(f.total)}`
            : f.received
              ? formatBytes(f.received)
              : "准备中";
        return `<div class="queue-item is-active">
          <div class="queue-item-main">
            <div class="queue-name" title="${escapeHtml(f.file || "")}">${escapeHtml(f.file || `msg ${f.id}`)}</div>
            <div class="queue-meta">msg ${escapeHtml(String(f.id))} · ${escapeHtml(size)} · ${escapeHtml(pct)}</div>
          </div>
          <div class="queue-badge">下载中</div>
        </div>`;
      })
      .join("")}</div>`;
  }
  if (items.length) {
    const section =
      kindN === "done"
        ? `已完成（显示 ${items.length}${r.total > items.length ? ` / ${r.total}` : ""}）`
        : kindN === "matches"
          ? `命中列表（显示 ${items.length}${r.total > items.length ? ` / ${r.total}` : ""}）`
          : `待下载（显示 ${items.length}${r.total > items.length ? ` / ${r.total}` : ""}）`;
    html += `<div class="queue-section-title">${section}</div>`;
    html += `<div class="queue-list">${_renderIndexFileRows(items)}</div>`;
  }
  body.innerHTML = html;
}

function openQueueModal(taskId) {
  return openTaskFilesModal(taskId, "queue");
}

function closeQueueModal() {
  const modal = $("queueModal");
  if (modal) modal.hidden = true;
  const body = $("queueModalBody");
  if (body) body.innerHTML = "";
  state._queueTaskId = null;
  state._filesKind = null;
  syncBodyModalLock();
}

function renderTask(t) {
  const statusClass = `status-${t.status || "pending"}`;
  const q = queueRemaining(t);
  const title = t.chat_title || t.chat_id || "";
  return `<div class="task" data-task-id="${t.id}">
    <div class="task-head">
      <div class="task-head-main">
        <p class="task-title" title="${escapeHtml(title)}">${escapeHtml(title)}</p>
      </div>
      <span class="status-pill ${statusClass}">${escapeHtml(statusLabel(t.status, t))}</span>
    </div>
    <div class="task-stats">
      <button type="button" class="task-stat-btn" data-role="match-stat" data-action="show-matches" data-id="${t.id}" title="查看标签命中">
        <span class="ui-ico ui-ico-tag" aria-hidden="true"></span>
        <strong data-role="match-count">${t.tag_match_count ?? 0}</strong>
        <span class="task-stat-label">命中</span>
      </button>
      <button type="button" class="task-stat-btn" data-role="done-stat" data-action="show-done" data-id="${t.id}" title="查看已处理">
        <span class="ui-ico ui-ico-check" aria-hidden="true"></span>
        <strong data-role="done-count">${t.tag_processed_count ?? 0}</strong>
        <span class="task-stat-label">已处理</span>
      </button>
      <button type="button" class="task-stat-btn" data-role="queue-stat" data-action="show-queue" data-id="${t.id}" title="查看待下载队列">
        <span class="ui-ico ui-ico-queue" aria-hidden="true"></span>
        <strong data-role="queue-count">${q}</strong>
        <span class="task-stat-label">队列</span>
      </button>
    </div>
    <div class="task-actions">
      <button data-action="start" data-id="${t.id}" ${t.status === "running" ? "disabled" : ""}>
        <span class="ui-ico ui-ico-play" aria-hidden="true"></span><span>继续</span>
      </button>
      <button data-action="pause" data-id="${t.id}" ${t.status !== "running" ? "disabled" : ""}>
        <span class="ui-ico ui-ico-pause" aria-hidden="true"></span><span>暂停</span>
      </button>
      <button type="button" class="ghost" data-action="open-settings" data-id="${t.id}">
        <span class="ui-ico ui-ico-gear" aria-hidden="true"></span><span>设置</span>
      </button>
      <button class="danger" data-action="delete" data-id="${t.id}">
        <span class="ui-ico ui-ico-trash" aria-hidden="true"></span><span>删除</span>
      </button>
    </div>
    ${t.last_error ? `<p class="msg err is-visible"><span class="msg-icon" aria-hidden="true"></span><span class="msg-text">${escapeHtml(t.last_error)}</span></p>` : ""}
    ${renderLiveProgress(t)}
    ${renderTaskLog(t.last_log, t.id)}
  </div>`;
}

function startKeepalive() {
  if (state._keepAliveTimer) return;
  // Sliding cookie refresh — reduces iOS “woke up and logged out”
  state._keepAliveTimer = setInterval(() => {
    if (document.hidden) return;
    if ($("stageApp")?.hidden) return;
    fetch("/api/auth/web-session", { credentials: "same-origin" }).catch(() => {});
  }, 4 * 60 * 1000);
}

function stopKeepalive() {
  if (state._keepAliveTimer) {
    clearInterval(state._keepAliveTimer);
    state._keepAliveTimer = null;
  }
}

function tasksPollIntervalMs() {
  const tasks = state.tasks || [];
  const busy = tasks.some(
    (t) => t.status === "running" || (t.live && t.live.phase === "indexing")
  );
  return busy ? 2000 : 5000;
}

function scheduleTasksPoll() {
  stopPolling();
  const tick = () => {
    if (document.hidden) {
      state.pollTimer = setTimeout(tick, 8000);
      return;
    }
    if (!isOverlayBlockingPoll() && !state._tasksInflight) {
      loadTasks().catch(() => {});
    }
    state.pollTimer = setTimeout(tick, tasksPollIntervalMs());
  };
  state.pollTimer = setTimeout(tick, tasksPollIntervalMs());
}

function startPolling() {
  scheduleTasksPoll();
  startKeepalive();
  // iOS Safari freezes timers in background — soft resume when tab visible again
  if (!state._resumeBound) {
    state._resumeBound = true;
    let lastResume = 0;
    const resume = (ev) => {
      if (document.hidden) return;
      // bfcache restore: only restart timers, skip heavy refetch churn
      if (ev && ev.type === "pageshow" && ev.persisted) {
        if ($("stageApp") && !$("stageApp").hidden && !state.pollTimer) {
          startPolling();
        }
        return;
      }
      const now = Date.now();
      if (now - lastResume < 2500) return;
      lastResume = now;
      if ($("stageApp") && !$("stageApp").hidden) {
        if (!state.pollTimer) startPolling();
        loadTasks().catch(() => {});
        // TG chip: only refresh if we don't already have a good status
        if (!state._tgLastOk || !state._tgLastOk.authorized) {
          refreshTgState().catch(() => {});
        }
      }
    };
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) resume({ type: "visibilitychange" });
    });
    window.addEventListener("pageshow", resume);
    window.addEventListener("online", resume);
    // Do NOT bind window "focus" — iOS fires it constantly and feels like full refresh
  }
}

function stopPolling() {
  if (state.pollTimer) {
    clearTimeout(state.pollTimer);
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
  // Keepalive is stopped only on logout (showWebLogin); polling pause should not kill session refresh
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

bootstrap().catch((e) => {
  console.error(e);
  showWebLogin();
  setMsg($("webAuthMsg"), e.message || String(e), "err");
});
