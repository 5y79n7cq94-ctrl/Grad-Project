const LEADERBOARD_LIMIT = 40;
const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const SESSION_QUERY_KEYS = ["first_name", "last_name", "email", "position", "role", "token"];

const CATEGORY_LABELS = {
  entertainment: "Concert / Sport",
  experience: "Experience",
  food: "Food",
  shopping: "Shopping",
  exhibition: "Exhibition",
  accommodation: "Hotel",
};

const PLATFORM_LABELS = {
  wb: "Weibo",
  fb: "Facebook",
};

const STATUS_META = {
  completed: {
    label: "Completed",
    className: "status-completed",
    detail: "This weekly snapshot already has analyzed clusters.",
  },
  to_be_analyzed: {
    label: "To Be Analyzed",
    className: "status-updated",
    detail: "Raw posts exist in the database, but weekly clustering still needs to run.",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
    detail: "No raw posts exist for this week yet. Update Database first.",
  },
  future: {
    label: "Future",
    className: "status-future",
    detail: "Future weeks cannot be selected yet.",
  },
};

const UPDATE_CALENDAR_META = {
  future: {
    label: "Future",
    className: "status-future",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
  },
  updated: {
    label: "Updated",
    className: "status-completed",
  },
};

const CALENDAR_MODE_CONFIG = {
  filter: {
    eyebrow: "Weekly Heat Filter",
    title: "Pick Week",
    helper:
      "Choose one fixed Sunday to Saturday window for the leaderboard filter. Pick Week shows To Be Updated, To Be Analyzed, Completed, and Future weeks.",
    confirmLabel: "Use This Week",
    emptyLabel: "No week selected",
    emptyDetail: "Choose one fixed Sunday to Saturday week to refresh the leaderboard and heat overview.",
    selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed"]),
    legend: [
      { label: "To Be Updated", className: "swatch-available" },
      { label: "To Be Analyzed", className: "swatch-updated" },
      { label: "Completed", className: "swatch-imported" },
      { label: "Future", className: "swatch-future" },
    ],
  },
  update: {
    eyebrow: "Weekly Full-Web Heat Analysis",
    title: "Update Database",
    helper:
      "Choose one fixed Sunday to Saturday window to crawl and ingest raw posts. Update Database only distinguishes Future, To Be Updated, and Updated weeks.",
    confirmLabel: "Update This Week",
    emptyLabel: "No week selected",
    emptyDetail: "Click a blue fixed week to trigger crawling and ingest for that platform.",
    selectableStatuses: new Set(["to_be_updated"]),
    legend: [
      { label: "To Be Updated", className: "swatch-available" },
      { label: "Updated", className: "swatch-imported" },
      { label: "Future", className: "swatch-future" },
    ],
  },
};

const state = {
  boardType: "event",
  platform: "wb",
  sortMetric: "heat_score",
  selectedEvent: "",
  selectedWeek: null,
  calendarMode: "filter",
  calendarSelectedWeek: null,
  windows: [],
  monthCursor: new Date(),
  latestJob: null,
  pollingJobId: "",
};

const elements = {
  heatDbPathLabel: document.getElementById("heatDbPathLabel"),
  heatOverviewGrid: document.getElementById("heatOverviewGrid"),
  leaderboardTableBody: document.getElementById("leaderboardTableBody"),
  leaderboardTitle: document.getElementById("leaderboardTitle"),
  leaderboardSubtitle: document.getElementById("leaderboardSubtitle"),
  leaderboardCounter: document.getElementById("leaderboardCounter"),
  trendPageLink: document.getElementById("trendPageLink"),
  marketReportLink: document.getElementById("marketReportLink"),
  eventTabButton: document.getElementById("eventTabButton"),
  topicTabButton: document.getElementById("topicTabButton"),
  platformSelect: document.getElementById("platformSelect"),
  sortMetricSelect: document.getElementById("sortMetricSelect"),
  activeWeekLabel: document.getElementById("activeWeekLabel"),
  activeWeekSubLabel: document.getElementById("activeWeekSubLabel"),
  heatJobStatusLabel: document.getElementById("heatJobStatusLabel"),
  heatJobDetailLabel: document.getElementById("heatJobDetailLabel"),
  updateDatabaseButton: document.getElementById("updateDatabaseButton"),
  openSnapshotCalendarButton: document.getElementById("openSnapshotCalendarButton"),
  sidebarUpdateWeekButton: document.getElementById("sidebarUpdateWeekButton"),
  sidebarRunAnalysisButton: document.getElementById("sidebarRunAnalysisButton"),
  confirmUpdateButton: document.getElementById("confirmUpdateButton"),
  updateCalendarModal: document.getElementById("updateCalendarModal"),
  updateCalendarBackdrop: document.getElementById("updateCalendarBackdrop"),
  closeCalendarButton: document.getElementById("closeCalendarButton"),
  previousMonthButton: document.getElementById("previousMonthButton"),
  nextMonthButton: document.getElementById("nextMonthButton"),
  calendarMonthLabel: document.getElementById("calendarMonthLabel"),
  calendarWeekdayRow: document.getElementById("calendarWeekdayRow"),
  calendarGrid: document.getElementById("calendarGrid"),
  calendarModeEyebrow: document.getElementById("calendarModeEyebrow"),
  calendarModeTitle: document.getElementById("calendarModeTitle"),
  calendarModeHelper: document.getElementById("calendarModeHelper"),
  calendarSelectionLabel: document.getElementById("calendarSelectionLabel"),
  calendarSelectionDetail: document.getElementById("calendarSelectionDetail"),
  calendarLegend: document.getElementById("calendarLegend"),
  snapshotWindowList: document.getElementById("snapshotWindowList"),
  emptyTemplate: document.getElementById("leaderboardEmptyStateTemplate"),
};

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function postJson(url) {
  return requestJson(url, { method: "POST" });
}

function clearNode(node) {
  if (!node) {
    return;
  }
  while (node.firstChild) {
    node.removeChild(node.firstChild);
  }
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value || 0);
}

function formatScore(value) {
  return Number(value || 0).toFixed(2);
}

function clipText(value, maxLength = 132) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 1)}...`;
}

function getSessionParams() {
  const search = new URLSearchParams(window.location.search);
  const firstName = search.get("first_name");
  if (!firstName) {
    return null;
  }
  return {
    first_name: firstName,
    last_name: search.get("last_name") || "",
    email: search.get("email") || "",
    position: search.get("position") || "",
    role: search.get("role") || "user",
    token: search.get("token") || "",
  };
}

function appendSessionParams(url) {
  const session = getSessionParams();
  if (!session) {
    return url;
  }
  SESSION_QUERY_KEYS.forEach((key) => {
    if (session[key]) {
      url.searchParams.set(key, session[key]);
    }
  });
  return url;
}

function enforceSystemSession() {
  const session = getSessionParams();
  if (!session) {
    window.location.href = "/login_page.html";
    throw new Error("Missing login session. Redirecting to login page.");
  }
  if (session.position === "IT Admin") {
    const adminUrl = new URL("/admin_page.html", window.location.origin);
    appendSessionParams(adminUrl);
    window.location.href = `${adminUrl.pathname}${adminUrl.search}`;
    throw new Error("IT Admin should use the admin page.");
  }
  return session;
}

function syncSessionLinks() {
  const marketReportUrl = appendSessionParams(new URL("/project", window.location.origin));
  if (elements.marketReportLink) {
    elements.marketReportLink.href = `${marketReportUrl.pathname}${marketReportUrl.search}`;
  }
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No week selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function getBoardTypeLabel() {
  return state.boardType === "topic" ? "Topic" : "Event";
}

function getCurrentStatusMeta() {
  return STATUS_META[state.selectedWeek?.status] || STATUS_META.to_be_updated;
}

function getCalendarModeConfig() {
  return CALENDAR_MODE_CONFIG[state.calendarMode] || CALENDAR_MODE_CONFIG.filter;
}

function setStatus(status, detail) {
  elements.heatJobStatusLabel.textContent = status;
  elements.heatJobDetailLabel.textContent = detail;
}

function getHeatTone(value) {
  if (value >= 4) return { flames: "🔥🔥🔥", className: "heat-hot" };
  if (value >= 3) return { flames: "🔥🔥", className: "heat-warm" };
  return { flames: "🔥", className: "heat-mild" };
}

function sortItems(items) {
  const metric = state.sortMetric;
  return [...items]
    .sort((left, right) => {
      const rightValue = Number(right[metric] || 0);
      const leftValue = Number(left[metric] || 0);
      if (rightValue !== leftValue) {
        return rightValue - leftValue;
      }
      return Number(right.heat_score || 0) - Number(left.heat_score || 0);
    })
    .slice(0, LEADERBOARD_LIMIT);
}

function syncTrendLink() {
  const base = appendSessionParams(new URL("/full-web-heat-analysis/trends", window.location.origin));
  if (state.selectedEvent) {
    base.searchParams.set("event", state.selectedEvent);
  }
  if (state.platform) {
    base.searchParams.set("platform", state.platform);
  }
  if (state.selectedWeek?.week_start && state.selectedWeek?.week_end) {
    base.searchParams.set("week_start", state.selectedWeek.week_start);
    base.searchParams.set("week_end", state.selectedWeek.week_end);
  }
  elements.trendPageLink.href = `${base.pathname}${base.search}`;
}

function setSelectedWeek(week) {
  state.selectedWeek = week || null;
  const statusMeta = getCurrentStatusMeta();
  elements.activeWeekLabel.textContent = state.selectedWeek
    ? formatWeekLabel(state.selectedWeek.week_start, state.selectedWeek.week_end)
    : "No week selected";
  if (!state.selectedWeek) {
    elements.activeWeekSubLabel.textContent = "Choose one weekly snapshot after selecting a platform.";
  } else {
    elements.activeWeekSubLabel.textContent = `${statusMeta.label}. ${statusMeta.detail}`;
  }
  updateActionButtons();
  syncTrendLink();
}

function updateActionButtons() {
  const status = state.selectedWeek?.status || "";
  const selectedWeekLabel = state.selectedWeek
    ? formatWeekLabel(state.selectedWeek.week_start, state.selectedWeek.week_end)
    : "No week selected";

  elements.sidebarRunAnalysisButton.disabled = !state.selectedWeek || status !== "to_be_analyzed";
  elements.sidebarUpdateWeekButton.disabled = !state.selectedWeek || status !== "to_be_updated";

  if (!state.selectedWeek) {
    setStatus("Select a week", "Choose one weekly snapshot from the left-side date filter.");
    return;
  }

  if (status === "completed") {
    setStatus(
      "Completed",
      `${selectedWeekLabel} already has analyzed ${state.boardType} clusters. You can inspect the leaderboard or switch fixed weekly windows.`
    );
  } else if (status === "to_be_analyzed") {
    setStatus(
      "To Be Analyzed",
      `${selectedWeekLabel} is already updated in the database. Click Run Analysis to build weekly clusters.`
    );
  } else if (status === "to_be_updated") {
    setStatus(
      "To Be Updated",
      `${selectedWeekLabel} has no stored raw posts yet. Click Update Database to crawl and ingest this fixed weekly window first.`
    );
  }
}

function renderOverview(items) {
  clearNode(elements.heatOverviewGrid);
  const totalEngagement = items.reduce((sum, item) => sum + Number(item.total_engagement || 0), 0);
  const totalDiscussion = items.reduce((sum, item) => sum + Number(item.discussion_total || 0), 0);
  const totalPosts = items.reduce((sum, item) => sum + Number(item.post_count || 0), 0);
  const topItem = items[0];
  const cards = [
    {
      label: "Platform",
      value: PLATFORM_LABELS[state.platform] || "Unknown",
      sub: `${getBoardTypeLabel()} leaderboard scope`,
    },
    {
      label: "Status",
      value: getCurrentStatusMeta().label,
      sub: getCurrentStatusMeta().detail,
    },
    {
      label: "Weekly Posts",
      value: formatNumber(state.selectedWeek?.post_count || 0),
      sub: "Raw posts in the selected week",
    },
    {
      label: "Top Title",
      value: clipText(topItem?.cluster_key || "No cluster", 34),
      sub: topItem ? `Heat ${formatScore(topItem.heat_score)}` : "No cluster built yet",
    },
    {
      label: "Engagement",
      value: formatNumber(totalEngagement),
      sub: "Visible leaderboard total",
    },
    {
      label: "Discussion",
      value: formatNumber(totalDiscussion),
      sub: "Visible leaderboard total",
    },
    {
      label: "Posts",
      value: formatNumber(totalPosts),
      sub: "Visible leaderboard total",
    },
  ];

  cards.forEach((card) => {
    const node = document.createElement("article");
    node.className = "overview-card";
    node.innerHTML = `
      <span>${card.label}</span>
      <strong>${card.value}</strong>
      <small>${card.sub}</small>
    `;
    elements.heatOverviewGrid.appendChild(node);
  });
}

function renderLeaderboard(items) {
  clearNode(elements.leaderboardTableBody);
  if (!items.length) {
    elements.leaderboardTableBody.appendChild(elements.emptyTemplate.content.cloneNode(true));
    elements.leaderboardCounter.textContent = "0 rows";
    return;
  }

  elements.leaderboardCounter.textContent = `${formatNumber(items.length)} rows`;
  items.forEach((item, index) => {
    const tone = getHeatTone(Number(item.heat_score || 0));
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="leaderboard-rank-cell"><span class="rank-pill">${index + 1}</span></td>
      <td class="heat-title-cell">
        <strong>${item.cluster_key}</strong>
        <p>${clipText(CATEGORY_LABELS[item.dashboard_category] || item.organizer_name || "", 92)}</p>
      </td>
      <td class="leaderboard-metric-cell">${PLATFORM_LABELS[item.platform] || item.platform || "-"}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.post_count)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.total_engagement)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.discussion_total)}</td>
      <td class="leaderboard-metric-cell">${formatNumber(item.unique_authors)}</td>
      <td class="heat-score-cell ${tone.className}">
        <span class="heat-flames">${tone.flames}</span>
        <strong>${formatScore(item.heat_score)}</strong>
      </td>
    `;
    row.addEventListener("click", () => {
      state.selectedEvent = item.cluster_key || "";
      syncTrendLink();
    });
    elements.leaderboardTableBody.appendChild(row);
  });
}

function renderSnapshotWindowList() {
  clearNode(elements.snapshotWindowList);
  if (!state.windows.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = "No weekly windows were found for the selected platform.";
    elements.snapshotWindowList.appendChild(empty);
    return;
  }

  state.windows.forEach((item) => {
    const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `snapshot-window-card ${statusMeta.className}${
      state.selectedWeek?.week_start === item.week_start && state.selectedWeek?.week_end === item.week_end ? " selected" : ""
    }`;
    button.innerHTML = `
      <div class="snapshot-window-head">
        <strong>${item.week_start.slice(5)} to ${item.week_end.slice(5)}</strong>
        <span class="snapshot-status-badge ${statusMeta.className}">${statusMeta.label}</span>
      </div>
      <p>${formatNumber(item.post_count || 0)} posts · fixed Sunday-Saturday window</p>
    `;
    button.addEventListener("click", async () => {
      setSelectedWeek(item);
      renderSnapshotWindowList();
      await fetchLeaderboardData();
    });
    elements.snapshotWindowList.appendChild(button);
  });
}

function openCalendar() {
  syncCalendarModal();
  renderCalendar();
  elements.updateCalendarModal.classList.remove("hidden");
  elements.updateCalendarModal.setAttribute("aria-hidden", "false");
}

function closeCalendar() {
  elements.updateCalendarModal.classList.add("hidden");
  elements.updateCalendarModal.setAttribute("aria-hidden", "true");
}

function startOfWeek(dateValue) {
  const copy = new Date(dateValue);
  const diff = copy.getDate() - copy.getDay();
  copy.setDate(diff);
  copy.setHours(0, 0, 0, 0);
  return copy;
}

function endOfWeek(dateValue) {
  const copy = startOfWeek(dateValue);
  copy.setDate(copy.getDate() + 6);
  return copy;
}

function toIsoDate(dateValue) {
  return `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, "0")}-${String(dateValue.getDate()).padStart(2, "0")}`;
}

function formatCalendarSelectionDetail(week) {
  if (!week) {
    return getCalendarModeConfig().emptyDetail;
  }

  if (state.calendarMode === "update") {
    return week.status === "to_be_updated"
      ? `This fixed Sunday-Saturday week is ready to crawl for ${PLATFORM_LABELS[state.platform]}.`
      : `${formatWeekLabel(week.week_start, week.week_end)} is already updated for ${PLATFORM_LABELS[state.platform]}.`;
  }

  const statusMeta = STATUS_META[week.status] || STATUS_META.to_be_updated;
  return `${statusMeta.label}. ${statusMeta.detail}`;
}

function syncCalendarSelectionSummary() {
  const config = getCalendarModeConfig();
  elements.calendarSelectionLabel.textContent = state.calendarSelectedWeek
    ? formatWeekLabel(state.calendarSelectedWeek.week_start, state.calendarSelectedWeek.week_end)
    : config.emptyLabel;
  elements.calendarSelectionDetail.textContent = formatCalendarSelectionDetail(state.calendarSelectedWeek);
  elements.confirmUpdateButton.disabled = !(
    state.calendarSelectedWeek && config.selectableStatuses.has(state.calendarSelectedWeek.status)
  );
}

function renderCalendarLegend() {
  clearNode(elements.calendarLegend);
  getCalendarModeConfig().legend.forEach((item) => {
    const node = document.createElement("span");
    node.className = "calendar-legend-item";
    node.innerHTML = `<span class="calendar-swatch ${item.className}"></span>${item.label}`;
    elements.calendarLegend.appendChild(node);
  });
}

function syncCalendarModal() {
  const config = getCalendarModeConfig();
  elements.calendarModeEyebrow.textContent = config.eyebrow;
  elements.calendarModeTitle.textContent = config.title;
  elements.calendarModeHelper.textContent = config.helper;
  elements.confirmUpdateButton.textContent = config.confirmLabel;
  renderCalendarLegend();
  syncCalendarSelectionSummary();
}

function setCalendarMode(mode) {
  state.calendarMode = mode;
  if (mode === "update") {
    state.calendarSelectedWeek =
      (state.selectedWeek?.status === "to_be_updated" && state.selectedWeek) ||
      state.windows.find((item) => item.status === "to_be_updated") ||
      null;
  } else {
    state.calendarSelectedWeek = state.selectedWeek;
  }
  syncCalendarModal();
}

function renderCalendar() {
  clearNode(elements.calendarWeekdayRow);
  clearNode(elements.calendarGrid);
  WEEKDAY_LABELS.forEach((label) => {
    const node = document.createElement("div");
    node.className = "calendar-weekday";
    node.textContent = label;
    elements.calendarWeekdayRow.appendChild(node);
  });

  const cursor = new Date(state.monthCursor.getFullYear(), state.monthCursor.getMonth(), 1);
  elements.calendarMonthLabel.textContent = cursor.toLocaleDateString("en-US", { month: "long", year: "numeric" });
  const gridStartDate = startOfWeek(cursor);
  const windowMap = new Map(state.windows.map((item) => [`${item.week_start}|${item.week_end}`, item]));

  for (let index = 0; index < 42; index += 1) {
    const current = new Date(gridStartDate);
    current.setDate(gridStartDate.getDate() + index);
    const weekStart = toIsoDate(startOfWeek(current));
    const weekEnd = toIsoDate(endOfWeek(current));
    const key = `${weekStart}|${weekEnd}`;
    const week = windowMap.get(key) || { week_start: weekStart, week_end: weekEnd, status: "future", post_count: 0 };
    const calendarStatus =
      state.calendarMode === "update"
        ? week.status === "to_be_updated"
          ? "to_be_updated"
          : week.status === "future"
            ? "future"
            : "updated"
        : week.status;
    const statusMeta =
      state.calendarMode === "update"
        ? UPDATE_CALENDAR_META[calendarStatus] || UPDATE_CALENDAR_META.future
        : STATUS_META[calendarStatus] || STATUS_META.future;
    const isCurrentMonth = current.getMonth() === cursor.getMonth();
    const isSelected =
      state.calendarSelectedWeek?.week_start === weekStart && state.calendarSelectedWeek?.week_end === weekEnd;
    const isSelectable = getCalendarModeConfig().selectableStatuses.has(week.status);

    const button = document.createElement("button");
    button.type = "button";
    button.className = `calendar-day ${statusMeta.className}${isCurrentMonth ? "" : " calendar-day-muted"}${isSelected ? " selected-week" : ""}`;
    button.innerHTML = `
      <span class="calendar-day-date">${current.getDate()}</span>
      <span class="calendar-day-range">${weekStart.slice(5)} to ${weekEnd.slice(5)}</span>
    `;
    if (isSelectable) {
      button.addEventListener("click", () => {
        state.calendarSelectedWeek = week;
        syncCalendarSelectionSummary();
        renderCalendar();
      });
    } else {
      button.disabled = true;
    }
    elements.calendarGrid.appendChild(button);
  }

  syncCalendarSelectionSummary();
}

async function loadWindows() {
  const payload = await requestJson(`/api/full-web-heat-analysis/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24`);
  state.windows = payload.items || [];

  if (state.selectedWeek) {
    const matched = state.windows.find(
      (item) => item.week_start === state.selectedWeek.week_start && item.week_end === state.selectedWeek.week_end
    );
    if (matched) {
      state.selectedWeek = matched;
    }
  }

  if (!state.selectedWeek) {
    const latestCompleted = state.windows.find((item) => item.status === "completed");
    const latestAnalyzable = state.windows.find((item) => item.status === "to_be_analyzed");
    const latestUpdate = state.windows.find((item) => item.status === "to_be_updated");
    setSelectedWeek(latestCompleted || latestAnalyzable || latestUpdate || null);
  } else {
    setSelectedWeek(state.selectedWeek);
  }

  renderSnapshotWindowList();
  if (state.calendarMode === "filter") {
    state.calendarSelectedWeek = state.selectedWeek;
  } else if (
    !state.calendarSelectedWeek ||
    !state.windows.some(
      (item) =>
        item.week_start === state.calendarSelectedWeek.week_start && item.week_end === state.calendarSelectedWeek.week_end
    )
  ) {
    state.calendarSelectedWeek = state.windows.find((item) => item.status === "to_be_updated") || null;
  }
  syncCalendarModal();
  renderCalendar();
}

async function fetchLeaderboardData() {
  const overview = await requestJson(`/api/full-web-heat-analysis/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`);
  elements.heatDbPathLabel.textContent = overview.db_path || "Unknown analytics database";

  if (!state.selectedWeek) {
    renderOverview([]);
    renderLeaderboard([]);
    return;
  }

  const query = new URLSearchParams({
    platform: state.platform,
    limit: "120",
    week_start: state.selectedWeek.week_start,
    week_end: state.selectedWeek.week_end,
  });
  const endpoint = state.boardType === "topic" ? "/api/full-web-heat-analysis/topic-clusters" : "/api/full-web-heat-analysis/event-clusters";
  const clusterPayload = await requestJson(`${endpoint}?${query.toString()}`);
  const items = sortItems(clusterPayload.items || []);
  renderOverview(items);
  renderLeaderboard(items);
  if (clusterPayload.total > 0) {
    state.selectedEvent = items[0]?.cluster_key || "";
  } else {
    state.selectedEvent = "";
  }
  syncTrendLink();
}

async function pollProjectJob(jobId) {
  state.pollingJobId = jobId;
  const tick = async () => {
    const job = await requestJson(`/api/full-web-heat-analysis/jobs/${jobId}`);
    state.latestJob = job;
    if (job.status === "queued" || job.status === "running") {
      setStatus("Updating Database", `Background job is crawling and syncing ${PLATFORM_LABELS[state.platform]} for the selected week.`);
      window.setTimeout(tick, 2000);
      return;
    }
    state.pollingJobId = "";
    if (job.status === "failed") {
      setStatus("Update failed", job.error || "The update job failed.");
      return;
    }
    await loadWindows();
    await fetchLeaderboardData();
    setStatus("Update completed", "This week is now in the database. If the status is To Be Analyzed, click Run Analysis next.");
  };
  await tick();
}

async function startUpdateForSelectedWeek() {
  const week = state.calendarSelectedWeek || state.selectedWeek;
  if (!week || week.status !== "to_be_updated") {
    return;
  }
  setSelectedWeek(week);
  const query = new URLSearchParams({
    platform: state.platform,
    week_start: week.week_start,
    week_end: week.week_end,
  });
  const job = await postJson(`/api/full-web-heat-analysis/update-week?${query.toString()}`);
  closeCalendar();
  await pollProjectJob(job.job_id);
}

async function confirmCalendarSelection() {
  if (!state.calendarSelectedWeek) {
    return;
  }

  if (state.calendarMode === "update") {
    await startUpdateForSelectedWeek();
    return;
  }

  setSelectedWeek(state.calendarSelectedWeek);
  renderSnapshotWindowList();
  closeCalendar();
  await fetchLeaderboardData();
}

async function runAnalysisForSelectedWeek() {
  if (!state.selectedWeek || state.selectedWeek.status !== "to_be_analyzed") {
    updateActionButtons();
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    replace: "true",
    week_start: state.selectedWeek.week_start,
    week_end: state.selectedWeek.week_end,
  });

  setStatus(
    "Running Analysis",
    `Building ${getBoardTypeLabel().toLowerCase()} clusters for ${PLATFORM_LABELS[state.platform]} ${formatWeekLabel(
      state.selectedWeek.week_start,
      state.selectedWeek.week_end
    )}.`
  );
  try {
    const result = await postJson(`/api/full-web-heat-analysis/run-analysis?${query.toString()}`);
    await loadWindows();
    await fetchLeaderboardData();
    setStatus(
      "Analysis completed",
      `${formatNumber(result.event_cluster_rows || 0)} event clusters and ${formatNumber(
        result.topic_cluster_rows || 0
      )} topic clusters are now ready for this week.`
    );
  } catch (error) {
    setStatus("Analysis failed", error.message);
  }
}

function bindEvents() {
  elements.eventTabButton.addEventListener("click", async () => {
    state.boardType = "event";
    elements.eventTabButton.classList.add("active");
    elements.eventTabButton.classList.remove("secondary");
    elements.topicTabButton.classList.remove("active");
    elements.topicTabButton.classList.add("secondary");
    elements.leaderboardTitle.textContent = "Event Leaderboard";
    elements.leaderboardSubtitle.textContent =
      "Select one weekly snapshot on the left, then compare event clusters by heat, posts, engagement, discussion, or unique authors.";
    await fetchLeaderboardData();
  });

  elements.topicTabButton.addEventListener("click", async () => {
    state.boardType = "topic";
    elements.topicTabButton.classList.add("active");
    elements.topicTabButton.classList.remove("secondary");
    elements.eventTabButton.classList.remove("active");
    elements.eventTabButton.classList.add("secondary");
    elements.leaderboardTitle.textContent = "Topic Leaderboard";
    elements.leaderboardSubtitle.textContent =
      "Select one weekly snapshot on the left, then compare broader discussion topics using the same weekly window.";
    await fetchLeaderboardData();
  });

  elements.platformSelect.addEventListener("change", async (event) => {
    state.platform = event.target.value || "wb";
    state.selectedWeek = null;
    await loadWindows();
    await fetchLeaderboardData();
  });

  elements.sortMetricSelect.addEventListener("change", async (event) => {
    state.sortMetric = event.target.value;
    await fetchLeaderboardData();
  });

  elements.updateDatabaseButton.addEventListener("click", () => {
    setCalendarMode("update");
    openCalendar();
  });
  elements.sidebarUpdateWeekButton.addEventListener("click", () => {
    setCalendarMode("update");
    openCalendar();
  });
  elements.openSnapshotCalendarButton.addEventListener("click", () => {
    setCalendarMode("filter");
    openCalendar();
  });
  elements.closeCalendarButton.addEventListener("click", closeCalendar);
  elements.updateCalendarBackdrop.addEventListener("click", closeCalendar);
  elements.previousMonthButton.addEventListener("click", () => {
    state.monthCursor = new Date(state.monthCursor.getFullYear(), state.monthCursor.getMonth() - 1, 1);
    renderCalendar();
  });
  elements.nextMonthButton.addEventListener("click", () => {
    state.monthCursor = new Date(state.monthCursor.getFullYear(), state.monthCursor.getMonth() + 1, 1);
    renderCalendar();
  });
  elements.confirmUpdateButton.addEventListener("click", confirmCalendarSelection);
  elements.sidebarRunAnalysisButton.addEventListener("click", runAnalysisForSelectedWeek);
}

async function bootstrap() {
  enforceSystemSession();
  syncSessionLinks();
  const url = new URL(window.location.href);
  state.platform = url.searchParams.get("platform") || "wb";
  elements.platformSelect.value = state.platform;
  bindEvents();
  await loadWindows();
  if (url.searchParams.get("week_start") && url.searchParams.get("week_end")) {
    const matched = state.windows.find(
      (item) =>
        item.week_start === url.searchParams.get("week_start") &&
        item.week_end === url.searchParams.get("week_end")
    );
    if (matched) {
      setSelectedWeek(matched);
      renderSnapshotWindowList();
    }
  }
  await fetchLeaderboardData();
}

bootstrap().catch((error) => {
  elements.heatDbPathLabel.textContent = `Load failed: ${error.message}`;
  renderLeaderboard([]);
  setStatus("Load failed", error.message);
});
