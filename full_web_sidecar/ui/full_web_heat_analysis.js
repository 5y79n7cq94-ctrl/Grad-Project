const LEADERBOARD_LIMIT = 40;
const WEEKDAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const API_BASE = "/api/full-web-heat-analysis";
const HEAT_ROUTE = "/full-web-heat-analysis";
const TRENDS_ROUTE = "/full-web-heat-analysis/trends";
const MARKET_ROUTE = "/operation_panel.html";
const LOGIN_ROUTE = "/login_page.html";
const SESSION_KEYS = ["first_name", "last_name", "email", "position", "role", "token"];

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
    eyebrow: "Weekly Heat Analysis",
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

const QUARTERLY_PENDING_COPY =
  "Quarterly reporting is not available yet. Full-Web collection started on 2026-03-01, and the first complete Q2 2026 report will be available after June 2026.";

const state = {
  boardType: "event",
  platform: "wb",
  windowMode: "monthly",
  sortMetric: "heat_score",
  selectedEvent: "",
  selectedWeek: null,
  calendarMode: "filter",
  calendarSelectedWeek: null,
  calendarNotice: "",
  windows: [],
  updateWindows: [],
  monthCursor: new Date(),
  latestJob: null,
  pollingJobId: "",
  leaderboardLoading: false,
  contextRow: null,
  lastRenderedItems: [],
  overviewCache: {},
};

const elements = {
  heatDbPathLabel: document.getElementById("heatDbPathLabel"),
  heatOverviewGrid: document.getElementById("heatOverviewGrid"),
  leaderboardTableBody: document.getElementById("leaderboardTableBody"),
  leaderboardTitle: document.getElementById("leaderboardTitle"),
  leaderboardSubtitle: document.getElementById("leaderboardSubtitle"),
  leaderboardCounter: document.getElementById("leaderboardCounter"),
  leaderboardLoadingHint: document.getElementById("leaderboardLoadingHint"),
  leaderboardBusyOverlay: document.getElementById("leaderboardBusyOverlay"),
  leaderboardBusyTitle: document.getElementById("leaderboardBusyTitle"),
  leaderboardBusyDetail: document.getElementById("leaderboardBusyDetail"),
  trendPageLink: document.getElementById("trendPageLink"),
  backToMarketLink: document.getElementById("backToMarketLink"),
  eventTabButton: document.getElementById("eventTabButton"),
  topicTabButton: document.getElementById("topicTabButton"),
  platformSelect: document.getElementById("platformSelect"),
  sortMetricSelect: document.getElementById("sortMetricSelect"),
  windowModeSelect: document.getElementById("windowModeSelect"),
  activeWindowEyebrow: document.getElementById("activeWindowEyebrow"),
  activeWeekLabel: document.getElementById("activeWeekLabel"),
  activeWeekSubLabel: document.getElementById("activeWeekSubLabel"),
  snapshotFilterLabel: document.getElementById("snapshotFilterLabel"),
  snapshotFilterCopy: document.getElementById("snapshotFilterCopy"),
  heatJobStatusLabel: document.getElementById("heatJobStatusLabel"),
  heatJobDetailLabel: document.getElementById("heatJobDetailLabel"),
  updateDatabaseButton: document.getElementById("updateDatabaseButton"),
  openSnapshotCalendarButton: document.getElementById("openSnapshotCalendarButton"),
  sidebarRunAnalysisButton: document.getElementById("sidebarRunAnalysisButton"),
  confirmUpdateButton: document.getElementById("confirmUpdateButton"),
  updateCalendarModal: document.getElementById("updateCalendarModal"),
  updateCalendarBackdrop: document.getElementById("updateCalendarBackdrop"),
  closeCalendarButton: document.getElementById("closeCalendarButton"),
  previousMonthButton: document.getElementById("previousMonthButton"),
  nextMonthButton: document.getElementById("nextMonthButton"),
  calendarToolbar: document.getElementById("calendarToolbar"),
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
  clusterContextMenu: document.getElementById("clusterContextMenu"),
  clusterMarkNoiseButton: document.getElementById("clusterMarkNoiseButton"),
  clusterMergeButton: document.getElementById("clusterMergeButton"),
  clusterMergeModal: document.getElementById("clusterMergeModal"),
  clusterMergeBackdrop: document.getElementById("clusterMergeBackdrop"),
  closeClusterMergeButton: document.getElementById("closeClusterMergeButton"),
  cancelClusterMergeButton: document.getElementById("cancelClusterMergeButton"),
  clusterMergeTitle: document.getElementById("clusterMergeTitle"),
  clusterMergeHelper: document.getElementById("clusterMergeHelper"),
  clusterMergeSourceHeading: document.getElementById("clusterMergeSourceHeading"),
  clusterMergeTargetHeading: document.getElementById("clusterMergeTargetHeading"),
  clusterMergeSourceLabel: document.getElementById("clusterMergeSourceLabel"),
  clusterMergeTargetSelect: document.getElementById("clusterMergeTargetSelect"),
  confirmClusterMergeButton: document.getElementById("confirmClusterMergeButton"),
  clusterNoiseModal: document.getElementById("clusterNoiseModal"),
  clusterNoiseBackdrop: document.getElementById("clusterNoiseBackdrop"),
  closeClusterNoiseButton: document.getElementById("closeClusterNoiseButton"),
  cancelClusterNoiseButton: document.getElementById("cancelClusterNoiseButton"),
  clusterNoiseTitle: document.getElementById("clusterNoiseTitle"),
  clusterNoiseHelper: document.getElementById("clusterNoiseHelper"),
  clusterNoiseSourceHeading: document.getElementById("clusterNoiseSourceHeading"),
  clusterNoiseSourceLabel: document.getElementById("clusterNoiseSourceLabel"),
  confirmClusterNoiseButton: document.getElementById("confirmClusterNoiseButton"),
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

function getSessionParams() {
  const url = new URL(window.location.href);
  const params = new URLSearchParams();
  for (const key of SESSION_KEYS) {
    const value = url.searchParams.get(key);
    if (value) {
      params.set(key, value);
    }
  }
  return params;
}

function requireSession() {
  const sessionParams = getSessionParams();
  if (!sessionParams.get("first_name")) {
    window.location.href = LOGIN_ROUTE;
    throw new Error("Login session missing.");
  }
  return sessionParams;
}

function buildUrlWithSession(pathname, extra = {}) {
  const url = new URL(pathname, window.location.origin);
  const params = getSessionParams();
  params.forEach((value, key) => {
    url.searchParams.set(key, value);
  });
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return `${url.pathname}${url.search}`;
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

function isMonthlyMode() {
  return state.windowMode === "monthly";
}

function isQuarterlyMode() {
  return state.windowMode === "quarterly";
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No week selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function formatMonthLabel(monthKey) {
  if (!monthKey) {
    return "No month selected";
  }
  const [year, month] = String(monthKey).split("-");
  if (!year || !month) {
    return monthKey;
  }
  return `${year}-${month}`;
}

function formatMonthHeading(monthKey) {
  const [year, month] = String(monthKey || "").split("-");
  if (!year || !month) {
    return monthKey || "Unknown month";
  }
  const dateValue = new Date(Number(year), Number(month) - 1, 1);
  return dateValue.toLocaleDateString("en-US", { month: "long", year: "numeric" });
}

function formatSelectedWindowLabel(windowValue) {
  if (!windowValue) {
    if (isQuarterlyMode()) {
      return "No quarter selected";
    }
    return isMonthlyMode() ? "No month selected" : "No week selected";
  }
  if (isQuarterlyMode()) {
    return windowValue.quarter_key || "No quarter selected";
  }
  return isMonthlyMode()
    ? formatMonthLabel(windowValue.month_key)
    : formatWeekLabel(windowValue.week_start, windowValue.week_end);
}

function getBoardTypeLabel() {
  return state.boardType === "topic" ? "Topic" : "Event";
}

function getCurrentStatusMeta() {
  return STATUS_META[state.selectedWeek?.status] || STATUS_META.to_be_updated;
}

function getCalendarModeConfig() {
  if (state.calendarMode === "update") {
    return CALENDAR_MODE_CONFIG.update;
  }
  if (isMonthlyMode()) {
    return {
      eyebrow: "Monthly Heat Filter",
      title: "Pick Month",
      helper: "Choose one calendar month for the leaderboard filter. Pick Month shows To Be Updated, To Be Analyzed, Completed, and Future monthly windows.",
      confirmLabel: "Use This Month",
      emptyLabel: "No month selected",
      emptyDetail: "Choose one calendar month to refresh the leaderboard and heat overview.",
      selectableStatuses: new Set(["to_be_updated", "to_be_analyzed", "completed"]),
      legend: [
        { label: "To Be Updated", className: "swatch-available" },
        { label: "To Be Analyzed", className: "swatch-updated" },
        { label: "Completed", className: "swatch-imported" },
        { label: "Future", className: "swatch-future" },
      ],
    };
  }
  if (isQuarterlyMode()) {
    return {
      eyebrow: "Quarterly Heat Filter",
      title: "Pick Quarter",
      helper:
        "Quarterly reporting is reserved for complete calendar quarters. Full-Web collection begins on 2026-03-01, so the first complete quarterly report will be available after June 2026.",
      confirmLabel: "Use This Quarter",
      emptyLabel: "No quarter selected",
      emptyDetail: "Quarterly reporting is not available yet because there is not yet a complete quarter of Full-Web data.",
      selectableStatuses: new Set(),
      legend: [{ label: "Quarterly report pending", className: "swatch-future" }],
    };
  }
  return CALENDAR_MODE_CONFIG.filter;
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

function getSortMetricValue(item, metric) {
  const metricAlias = {
    heat_score: "heat_score",
    post_count: "post_count",
    engagement_total: "total_engagement",
    discussion_total: "discussion_total",
    unique_authors: "unique_authors",
  };
  const fieldName = metricAlias[metric] || metric;
  return Number(item?.[fieldName] || 0);
}

function sortItems(items) {
  const metric = state.sortMetric;
  return [...items]
    .sort((left, right) => {
      const rightValue = getSortMetricValue(right, metric);
      const leftValue = getSortMetricValue(left, metric);
      if (rightValue !== leftValue) {
        return rightValue - leftValue;
      }
      return Number(right.heat_score || 0) - Number(left.heat_score || 0);
    })
    .slice(0, LEADERBOARD_LIMIT);
}

function setLeaderboardLoading(isLoading, message = "Refreshing the leaderboard for the current filters...") {
  state.leaderboardLoading = isLoading;
  if (!elements.leaderboardLoadingHint) {
    return;
  }
  elements.leaderboardLoadingHint.textContent = message;
  elements.leaderboardLoadingHint.classList.toggle("hidden", !isLoading);
}

function setPanelBusy(isBusy, title = "Data is loading...", detail = "Please wait while the leaderboard refreshes.") {
  elements.leaderboardBusyOverlay?.classList.toggle("hidden", !isBusy);
  elements.leaderboardBusyOverlay?.setAttribute("aria-hidden", isBusy ? "false" : "true");
  if (elements.leaderboardBusyTitle) {
    elements.leaderboardBusyTitle.textContent = title;
  }
  if (elements.leaderboardBusyDetail) {
    elements.leaderboardBusyDetail.textContent = detail;
  }
}

function syncTrendLink() {
  elements.trendPageLink.href = buildUrlWithSession(TRENDS_ROUTE, {
    event: state.selectedEvent,
    platform: state.platform,
    window_mode: state.windowMode,
    month_key: isMonthlyMode() ? state.selectedWeek?.month_key : "",
    week_start: !isQuarterlyMode() ? state.selectedWeek?.week_start : "",
    week_end: !isQuarterlyMode() ? state.selectedWeek?.week_end : "",
  });
  if (elements.backToMarketLink) {
    elements.backToMarketLink.href = buildUrlWithSession(MARKET_ROUTE);
  }
}

function setSelectedWeek(week) {
  state.selectedWeek = week || null;
  const statusMeta = getCurrentStatusMeta();
  elements.activeWindowEyebrow.textContent = isQuarterlyMode() ? "Current Quarter" : isMonthlyMode() ? "Current Month" : "Current Week";
  elements.activeWeekLabel.textContent = formatSelectedWindowLabel(state.selectedWeek);
  if (!state.selectedWeek) {
    elements.activeWeekSubLabel.textContent = isQuarterlyMode()
      ? "Choose one quarterly snapshot after selecting a platform."
      : isMonthlyMode()
      ? "Choose one monthly snapshot after selecting a platform."
      : "Choose one weekly snapshot after selecting a platform.";
  } else {
    elements.activeWeekSubLabel.textContent = `${statusMeta.label}. ${statusMeta.detail}`;
  }
  updateActionButtons();
  syncTrendLink();
}

function updateActionButtons() {
  const status = state.selectedWeek?.status || "";
  const selectedWeekLabel = formatSelectedWindowLabel(state.selectedWeek);
  if (isQuarterlyMode()) {
    elements.sidebarRunAnalysisButton.disabled = true;
    elements.updateDatabaseButton.disabled = true;
    elements.openSnapshotCalendarButton.disabled = false;
    elements.openSnapshotCalendarButton.textContent = "Pick Quarter";
    elements.snapshotFilterLabel.textContent = "Quarterly Filter";
    elements.snapshotFilterCopy.textContent =
      "Quarterly reporting stays visible for planning, but Full-Web collection started on 2026-03-01. The first complete Q2 2026 report will be available after June 2026.";
    setStatus(
      "Quarterly report pending",
      state.calendarNotice || QUARTERLY_PENDING_COPY
    );
    return;
  }
  elements.sidebarRunAnalysisButton.disabled = !state.selectedWeek || !new Set(["to_be_analyzed", "completed"]).has(status);
  elements.updateDatabaseButton.disabled = false;
  elements.openSnapshotCalendarButton.disabled = false;
  elements.openSnapshotCalendarButton.textContent = isMonthlyMode() ? "Pick Month" : "Pick Week";
  elements.snapshotFilterLabel.textContent = isMonthlyMode() ? "Monthly Filter" : "Weekly Filter";
  elements.snapshotFilterCopy.textContent = isMonthlyMode()
    ? "Monthly snapshots are derived from your existing Full-Web database. Monthly mode supports `To Be Updated`, `To Be Analyzed`, and `Completed` months based on imported data."
    : "Fixed Sunday to Saturday windows only. Use this filter to switch between `To Be Updated`, `To Be Analyzed`, and `Completed` weeks.";

  if (!state.selectedWeek) {
    setStatus(
      isMonthlyMode() ? "Select a month" : "Select a week",
      isMonthlyMode()
        ? "Choose one monthly snapshot from the left-side date filter."
        : "Choose one weekly snapshot from the left-side date filter."
    );
    return;
  }

  if (status === "completed") {
    setStatus(
      "Completed",
      `${selectedWeekLabel} already has analyzed ${state.boardType} clusters. You can inspect the leaderboard or rerun analysis for this ${
        isMonthlyMode() ? "month" : "week"
      }.`
    );
  } else if (status === "to_be_analyzed") {
    setStatus(
      "To Be Analyzed",
      `${selectedWeekLabel} is already updated in the database. Click Run Analysis to build ${
        isMonthlyMode() ? "monthly" : "weekly"
      } clusters.`
    );
  } else if (status === "to_be_updated") {
    setStatus(
      "To Be Updated",
      isMonthlyMode()
        ? `${selectedWeekLabel} has no stored raw posts yet. Monthly analysis will become available after the related weeks are imported.`
        : `${selectedWeekLabel} has no stored raw posts yet. Click Update Database to crawl and ingest this fixed weekly window first.`
    );
  }
}

function renderOverview(items) {
  clearNode(elements.heatOverviewGrid);
  if (isQuarterlyMode()) {
    const cards = [
      {
        label: "Platform",
        value: PLATFORM_LABELS[state.platform] || "Unknown",
        sub: "Quarterly planning view",
      },
      {
        label: "Quarterly Status",
        value: "Not Available Yet",
        sub: "Quarterly reporting starts once a complete quarter is available.",
      },
      {
        label: "Current Quarter",
        value: state.selectedWeek?.quarter_key || "2026-Q2",
        sub: "Target quarter for the first full report",
      },
      {
        label: "Collection Start",
        value: "2026-03-01",
        sub: "Full-Web collection began in March 2026.",
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
    return;
  }
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
      label: isMonthlyMode() ? "Monthly Posts" : "Weekly Posts",
      value: formatNumber(state.selectedWeek?.post_count || 0),
      sub: isMonthlyMode() ? "Raw posts in the selected month" : "Raw posts in the selected week",
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
  state.lastRenderedItems = items;
  hideClusterContextMenu();
  if (isQuarterlyMode()) {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td colspan="8">
        <div class="empty-state quarterly-empty-card">
          <h4>Quarterly leaderboard not available yet</h4>
          <p>${state.calendarNotice || QUARTERLY_PENDING_COPY}</p>
        </div>
      </td>
    `;
    elements.leaderboardTableBody.appendChild(row);
    elements.leaderboardCounter.textContent = "Quarterly pending";
    return;
  }
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
    row.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      state.contextRow = item;
      showClusterContextMenu(event.clientX, event.clientY);
    });
    elements.leaderboardTableBody.appendChild(row);
  });
}

function syncLeaderboardCopy() {
  if (isQuarterlyMode()) {
    elements.leaderboardTitle.textContent = `Quarterly ${getBoardTypeLabel()} Leaderboard`;
    elements.leaderboardSubtitle.textContent =
      "Quarterly reporting is visible for planning only. This leaderboard will become available once a complete Q2 2026 window has been collected and analyzed.";
    return;
  }
  if (state.boardType === "topic") {
    elements.leaderboardTitle.textContent = "Topic Leaderboard";
    elements.leaderboardSubtitle.textContent =
      `Select one ${isMonthlyMode() ? "monthly" : "weekly"} snapshot on the left, then compare broader discussion topics using the same ${isMonthlyMode() ? "monthly" : "weekly"} window.`;
    return;
  }
  elements.leaderboardTitle.textContent = "Event Leaderboard";
  elements.leaderboardSubtitle.textContent =
    `Select one ${isMonthlyMode() ? "monthly" : "weekly"} snapshot on the left, then compare event clusters by heat, posts, engagement, discussion, or unique authors.`;
}

function renderSnapshotWindowList() {
  clearNode(elements.snapshotWindowList);
  if (isQuarterlyMode()) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent =
      state.calendarNotice || QUARTERLY_PENDING_COPY;
    elements.snapshotWindowList.appendChild(empty);
    return;
  }
  if (!state.windows.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = isMonthlyMode()
      ? "No monthly windows were found for the selected platform."
      : "No weekly windows were found for the selected platform.";
    elements.snapshotWindowList.appendChild(empty);
    return;
  }

  state.windows.forEach((item) => {
    const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `snapshot-window-card ${statusMeta.className}${
      (
        isMonthlyMode()
          ? state.selectedWeek?.month_key === item.month_key
          : state.selectedWeek?.week_start === item.week_start && state.selectedWeek?.week_end === item.week_end
      ) ? " selected" : ""
    }`;
    button.innerHTML = `
      <div class="snapshot-window-head">
        <strong>${isMonthlyMode() ? formatMonthLabel(item.month_key) : `${item.week_start.slice(5)} to ${item.week_end.slice(5)}`}</strong>
        <span class="snapshot-status-badge ${statusMeta.className}">${statusMeta.label}</span>
      </div>
      <p>${formatNumber(item.post_count || 0)} posts · ${
        isMonthlyMode() ? "calendar month window" : "fixed Sunday-Saturday window"
      }</p>
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

function hideClusterContextMenu() {
  elements.clusterContextMenu?.classList.add("hidden");
  elements.clusterContextMenu?.setAttribute("aria-hidden", "true");
}

function syncFeedbackCopy() {
  const boardLabel = getBoardTypeLabel();
  const lowerBoardLabel = boardLabel.toLowerCase();
  if (elements.clusterMarkNoiseButton) {
    elements.clusterMarkNoiseButton.textContent = `Mark ${boardLabel} as Noise`;
  }
  if (elements.clusterMergeButton) {
    elements.clusterMergeButton.textContent = `Merge Into Existing ${boardLabel}`;
  }
  if (elements.clusterMergeTitle) {
    elements.clusterMergeTitle.textContent = `Merge ${boardLabel}`;
  }
  if (elements.clusterMergeHelper) {
    elements.clusterMergeHelper.textContent = `Choose an existing ${lowerBoardLabel} to merge this cluster into. The leaderboard will treat them as one ${lowerBoardLabel} inside the selected window.`;
  }
  if (elements.clusterMergeSourceHeading) {
    elements.clusterMergeSourceHeading.textContent = `Source ${boardLabel}`;
  }
  if (elements.clusterMergeTargetHeading) {
    elements.clusterMergeTargetHeading.textContent = `Merge Into ${boardLabel}`;
  }
  if (elements.clusterNoiseTitle) {
    elements.clusterNoiseTitle.textContent = `Mark ${boardLabel} as Noise`;
  }
  if (elements.clusterNoiseHelper) {
    elements.clusterNoiseHelper.textContent = `Confirm that this ${lowerBoardLabel} should be treated as noise inside the selected window. It will be removed from the leaderboard and heat calculations for this scope.`;
  }
  if (elements.clusterNoiseSourceHeading) {
    elements.clusterNoiseSourceHeading.textContent = `Selected ${boardLabel}`;
  }
}

function showClusterContextMenu(clientX, clientY) {
  if (!elements.clusterContextMenu) {
    return;
  }
  syncFeedbackCopy();
  const menu = elements.clusterContextMenu;
  menu.style.left = `${clientX}px`;
  menu.style.top = `${clientY}px`;
  menu.classList.remove("hidden");
  menu.setAttribute("aria-hidden", "false");
}

function openClusterMergeModal() {
  if (!state.contextRow) {
    return;
  }
  syncFeedbackCopy();
  elements.clusterMergeSourceLabel.textContent = state.contextRow.cluster_key || "No event selected";
  clearNode(elements.clusterMergeTargetSelect);
  const options = state.lastRenderedItems
    .filter((item) => item.cluster_key !== state.contextRow.cluster_key)
    .map((item) => item.cluster_key);
  options.forEach((item) => {
    const option = document.createElement("option");
    option.value = item;
    option.textContent = clipText(item, 48);
    option.title = item;
    elements.clusterMergeTargetSelect.appendChild(option);
  });
  elements.confirmClusterMergeButton.disabled = options.length === 0;
  elements.clusterMergeModal.classList.remove("hidden");
  elements.clusterMergeModal.setAttribute("aria-hidden", "false");
}

function closeClusterMergeModal() {
  elements.clusterMergeModal.classList.add("hidden");
  elements.clusterMergeModal.setAttribute("aria-hidden", "true");
}

function openClusterNoiseModal() {
  if (!state.contextRow) {
    return;
  }
  syncFeedbackCopy();
  if (elements.clusterNoiseSourceLabel) {
    elements.clusterNoiseSourceLabel.textContent = state.contextRow.cluster_key || "No selection";
  }
  elements.clusterNoiseModal.classList.remove("hidden");
  elements.clusterNoiseModal.setAttribute("aria-hidden", "false");
}

function closeClusterNoiseModal() {
  elements.clusterNoiseModal.classList.add("hidden");
  elements.clusterNoiseModal.setAttribute("aria-hidden", "true");
}

async function submitClusterFeedback(action, targetClusterKey = "") {
  if (!state.contextRow) {
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    board_type: state.boardType,
    action,
    source_cluster_key: state.contextRow.cluster_key || "",
  });
  if (isMonthlyMode()) {
    query.set("month_key", state.selectedWeek?.month_key || "");
  } else if (isQuarterlyMode()) {
    query.set("quarter_key", state.selectedWeek?.quarter_key || "");
  } else {
    query.set("week_start", state.selectedWeek?.week_start || "");
    query.set("week_end", state.selectedWeek?.week_end || "");
  }
  if (targetClusterKey) {
    query.set("target_cluster_key", targetClusterKey);
  }
  setPanelBusy(true, "Applying feedback...", "Updating the current cluster view based on your feedback.");
  try {
    await postJson(`${API_BASE}/cluster-feedback?${query.toString()}`);
    await fetchLeaderboardData();
  } finally {
    setPanelBusy(false);
    hideClusterContextMenu();
    closeClusterMergeModal();
    closeClusterNoiseModal();
  }
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

function monthOverlapsWindow(monthKey, week) {
  const [year, month] = String(monthKey || "").split("-");
  const monthStart = new Date(Number(year), Number(month) - 1, 1);
  const monthEnd = new Date(Number(year), Number(month), 0);
  const weekStart = new Date(`${week.week_start}T00:00:00`);
  const weekEnd = new Date(`${week.week_end}T00:00:00`);
  return weekStart <= monthEnd && weekEnd >= monthStart;
}

function listMonthKeysForWindow(week) {
  const keys = new Set();
  const start = new Date(`${week.week_start}T00:00:00`);
  const end = new Date(`${week.week_end}T00:00:00`);
  const cursor = new Date(start.getFullYear(), start.getMonth(), 1);
  const last = new Date(end.getFullYear(), end.getMonth(), 1);
  while (cursor <= last) {
    keys.add(`${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`);
    cursor.setMonth(cursor.getMonth() + 1);
  }
  return [...keys];
}

function getWindowMonthKeys(sourceWindows) {
  const keys = new Set();
  if (state.calendarMode === "filter" && isMonthlyMode()) {
    sourceWindows.forEach((item) => {
      if (item.month_key) {
        keys.add(item.month_key);
      }
    });
  } else {
    sourceWindows.forEach((item) => {
      listMonthKeysForWindow(item).forEach((key) => keys.add(key));
    });
  }
  return [...keys].sort((left, right) => right.localeCompare(left));
}

function syncCalendarSelectionSummary() {
  const config = getCalendarModeConfig();
  elements.calendarSelectionLabel.textContent = state.calendarSelectedWeek
    ? formatSelectedWindowLabel(state.calendarSelectedWeek)
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
  if (elements.calendarToolbar) {
    elements.calendarToolbar.classList.add("hidden");
  }
  elements.calendarWeekdayRow.classList.add("hidden");
  renderCalendarLegend();
  syncCalendarSelectionSummary();
}

function setCalendarMode(mode) {
  state.calendarMode = mode;
  const sourceWindows = mode === "update" ? state.updateWindows : state.windows;
  if (mode === "update") {
    state.calendarSelectedWeek =
      sourceWindows.find((item) => item.status === "to_be_updated") ||
      null;
  } else {
    state.calendarSelectedWeek = state.selectedWeek;
  }
  syncCalendarModal();
}

function renderCalendar() {
  clearNode(elements.calendarWeekdayRow);
  clearNode(elements.calendarGrid);
  if (isQuarterlyMode()) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent =
      state.calendarNotice || QUARTERLY_PENDING_COPY;
    elements.calendarGrid.appendChild(empty);
    syncCalendarSelectionSummary();
    return;
  }
  const useMonthPicker = state.calendarMode === "filter" && isMonthlyMode();
  const sourceWindows = state.calendarMode === "update" ? state.updateWindows : state.windows;
  const monthKeys = getWindowMonthKeys(sourceWindows);
  elements.calendarGrid.classList.toggle("calendar-month-grid", false);
  elements.calendarGrid.classList.toggle("calendar-week-grid", false);
  elements.calendarGrid.classList.add("calendar-scroll-grid");
  elements.calendarMonthLabel.textContent = useMonthPicker ? "Available Months" : "Available Weeks";

  if (useMonthPicker) {
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Months</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
      const isSelected = state.calendarSelectedWeek?.month_key === item.month_key;
      const isSelectable = getCalendarModeConfig().selectableStatuses.has(item.status);
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-day-month">${formatMonthLabel(item.month_key)}</span>
        <span class="calendar-day-meta">${formatNumber(item.post_count || 0)} posts · calendar month window</span>
      `;
      if (isSelectable) {
        button.addEventListener("click", () => {
          state.calendarSelectedWeek = item;
          syncCalendarSelectionSummary();
          renderCalendar();
        });
      } else {
        button.disabled = true;
      }
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.calendarGrid.appendChild(section);
    syncCalendarSelectionSummary();
    return;
  }

  if (!monthKeys.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = "No weekly windows are available for this platform.";
    elements.calendarGrid.appendChild(empty);
    syncCalendarSelectionSummary();
    return;
  }

  monthKeys.forEach((monthKey) => {
    const visibleWeeks = sourceWindows.filter((item) => monthOverlapsWindow(monthKey, item));
    if (!visibleWeeks.length) {
      return;
    }
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">${formatMonthHeading(monthKey)}</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid";

    visibleWeeks.forEach((week) => {
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
      const isSelected =
        state.calendarSelectedWeek?.week_start === week.week_start && state.calendarSelectedWeek?.week_end === week.week_end;
      const isSelectable = getCalendarModeConfig().selectableStatuses.has(week.status);

      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-week-range">${week.week_start} to ${week.week_end}</span>
        <span class="calendar-day-meta">${formatNumber(week.post_count || 0)} posts · fixed Sunday-Saturday window</span>
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
      grid.appendChild(button);
    });

    section.appendChild(grid);
    elements.calendarGrid.appendChild(section);
  });

  syncCalendarSelectionSummary();
}

async function loadWindows() {
  const payload = await requestJson(
    `${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24&window_mode=${encodeURIComponent(
      state.windowMode
    )}`
  );
  state.windows = payload.items || [];
  state.calendarNotice = payload.message || "";

  if (state.selectedWeek) {
    const matched = state.windows.find((item) =>
      isMonthlyMode()
        ? item.month_key === state.selectedWeek.month_key
        : item.week_start === state.selectedWeek.week_start && item.week_end === state.selectedWeek.week_end
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

  if (isQuarterlyMode() && state.windows.length) {
    setSelectedWeek(state.windows[0]);
  }

  renderSnapshotWindowList();
  state.calendarSelectedWeek = state.selectedWeek;
  syncCalendarModal();
  renderCalendar();
  updateActionButtons();
}

async function loadUpdateWindows() {
  const payload = await requestJson(
    `${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24&window_mode=weekly`
  );
  state.updateWindows = payload.items || [];
  updateActionButtons();
}

async function refreshOverviewMeta(force = false) {
  if (!force && state.overviewCache[state.platform]) {
    const cached = state.overviewCache[state.platform];
    elements.heatDbPathLabel.textContent = cached.db_path || "Unknown analytics database";
    return cached;
  }
  const overview = await requestJson(`${API_BASE}/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`);
  state.overviewCache[state.platform] = overview;
  elements.heatDbPathLabel.textContent = overview.db_path || "Unknown analytics database";
  return overview;
}

async function fetchLeaderboardData() {
  setLeaderboardLoading(true);
  setPanelBusy(true, "Data is loading...", "Refreshing the leaderboard for the current filters.");
  try {
    elements.heatDbPathLabel.textContent = state.overviewCache[state.platform]?.db_path || "Loading analytics database...";
    refreshOverviewMeta().catch((error) => {
      elements.heatDbPathLabel.textContent = `Overview unavailable: ${error.message}`;
    });

    if (isQuarterlyMode()) {
      renderOverview([]);
      renderLeaderboard([]);
      elements.leaderboardCounter.textContent = "Quarterly pending";
      return;
    }

    if (!state.selectedWeek) {
      renderOverview([]);
      renderLeaderboard([]);
      return;
    }

    const query = new URLSearchParams({
      platform: state.platform,
      limit: "120",
    });
    if (isMonthlyMode()) {
      query.set("month_key", state.selectedWeek.month_key);
    } else {
      query.set("week_start", state.selectedWeek.week_start);
      query.set("week_end", state.selectedWeek.week_end);
    }
    const endpoint = state.boardType === "topic" ? `${API_BASE}/topic-clusters` : `${API_BASE}/event-clusters`;
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
  } finally {
    setLeaderboardLoading(false);
    setPanelBusy(false);
  }
}

async function pollProjectJob(jobId) {
  state.pollingJobId = jobId;
  const tick = async () => {
    const job = await requestJson(`${API_BASE}/jobs/${jobId}`);
    state.latestJob = job;
    if (job.status === "queued" || job.status === "running") {
      setStatus("Updating Database", `Background job is crawling and syncing ${PLATFORM_LABELS[state.platform]} for the selected week.`);
      setPanelBusy(true, "Updating database...", `Crawling and ingesting ${PLATFORM_LABELS[state.platform]} posts for the selected weekly window.`);
      window.setTimeout(tick, 2000);
      return;
    }
    state.pollingJobId = "";
    if (job.status === "failed") {
      setPanelBusy(false);
      setStatus("Update failed", job.error || "The update job failed.");
      return;
    }
    await loadUpdateWindows();
    await loadWindows();
    await fetchLeaderboardData();
    setStatus("Update completed", "This week is now in the database. If the status is To Be Analyzed, click Run Analysis next.");
    setPanelBusy(false);
  };
  await tick();
}

async function startUpdateForSelectedWeek() {
  const week = state.calendarSelectedWeek || state.updateWindows.find((item) => item.status === "to_be_updated") || null;
  if (!week || week.status !== "to_be_updated") {
    setStatus("Nothing to update", `No weekly window is currently available to update for ${PLATFORM_LABELS[state.platform]}.`);
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    week_start: week.week_start,
    week_end: week.week_end,
  });
  const job = await postJson(`${API_BASE}/update-week?${query.toString()}`);
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
  if (!state.selectedWeek || !new Set(["to_be_analyzed", "completed"]).has(state.selectedWeek.status)) {
    updateActionButtons();
    return;
  }
  const query = new URLSearchParams({
    platform: state.platform,
    replace: "true",
  });
  if (isMonthlyMode()) {
    query.set("month_key", state.selectedWeek.month_key);
  } else {
    query.set("week_start", state.selectedWeek.week_start);
    query.set("week_end", state.selectedWeek.week_end);
  }

  setStatus(
    "Running Analysis",
    `Building ${getBoardTypeLabel().toLowerCase()} clusters for ${PLATFORM_LABELS[state.platform]} ${formatSelectedWindowLabel(
      state.selectedWeek
    )}.`
  );
  setPanelBusy(
    true,
    "Analysis in progress...",
    `Checking imported ${isMonthlyMode() ? "monthly" : "weekly"} data for ${PLATFORM_LABELS[state.platform]} and building cluster results.`
  );
  try {
    const result = await postJson(`${API_BASE}/extract-events?${query.toString()}`);
    await loadWindows();
    await loadUpdateWindows();
    await fetchLeaderboardData();
    setStatus(
      "Analysis completed",
      `${formatNumber(result.event_cluster_rows || 0)} event clusters and ${formatNumber(
        result.topic_cluster_rows || 0
      )} topic clusters are now ready for this ${isMonthlyMode() ? "month" : "week"}.`
    );
  } catch (error) {
    setStatus("Analysis failed", error.message);
  } finally {
    setPanelBusy(false);
  }
}

function bindEvents() {
  elements.eventTabButton.addEventListener("click", async () => {
    state.boardType = "event";
    elements.eventTabButton.classList.add("active");
    elements.eventTabButton.classList.remove("secondary");
    elements.topicTabButton.classList.remove("active");
    elements.topicTabButton.classList.add("secondary");
    syncLeaderboardCopy();
    await fetchLeaderboardData();
  });

  elements.topicTabButton.addEventListener("click", async () => {
    state.boardType = "topic";
    elements.topicTabButton.classList.add("active");
    elements.topicTabButton.classList.remove("secondary");
    elements.eventTabButton.classList.remove("active");
    elements.eventTabButton.classList.add("secondary");
    syncLeaderboardCopy();
    await fetchLeaderboardData();
  });

  elements.platformSelect.addEventListener("change", async (event) => {
    state.platform = event.target.value || "wb";
    state.selectedWeek = null;
    await Promise.all([loadUpdateWindows(), loadWindows(), refreshOverviewMeta(true)]);
    await fetchLeaderboardData();
  });

  elements.windowModeSelect.addEventListener("change", async (event) => {
    state.windowMode = event.target.value || "monthly";
    state.selectedWeek = null;
    syncLeaderboardCopy();
    if (isMonthlyMode() || isQuarterlyMode()) {
      closeCalendar();
    }
    await Promise.all([loadUpdateWindows(), loadWindows(), refreshOverviewMeta(true)]);
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
  elements.openSnapshotCalendarButton.addEventListener("click", () => {
    setCalendarMode("filter");
    openCalendar();
  });
  elements.closeCalendarButton.addEventListener("click", closeCalendar);
  elements.updateCalendarBackdrop.addEventListener("click", closeCalendar);
  elements.confirmUpdateButton.addEventListener("click", confirmCalendarSelection);
  elements.sidebarRunAnalysisButton.addEventListener("click", runAnalysisForSelectedWeek);
  elements.clusterMarkNoiseButton?.addEventListener("click", () => {
    hideClusterContextMenu();
    openClusterNoiseModal();
  });
  elements.clusterMergeButton?.addEventListener("click", () => {
    hideClusterContextMenu();
    openClusterMergeModal();
  });
  elements.closeClusterMergeButton?.addEventListener("click", closeClusterMergeModal);
  elements.cancelClusterMergeButton?.addEventListener("click", closeClusterMergeModal);
  elements.clusterMergeBackdrop?.addEventListener("click", closeClusterMergeModal);
  elements.confirmClusterMergeButton?.addEventListener("click", async () => {
    const target = elements.clusterMergeTargetSelect?.value || "";
    if (!target) {
      return;
    }
    await submitClusterFeedback("merge", target);
  });
  elements.closeClusterNoiseButton?.addEventListener("click", closeClusterNoiseModal);
  elements.cancelClusterNoiseButton?.addEventListener("click", closeClusterNoiseModal);
  elements.clusterNoiseBackdrop?.addEventListener("click", closeClusterNoiseModal);
  elements.confirmClusterNoiseButton?.addEventListener("click", async () => {
    await submitClusterFeedback("noise");
  });
  document.addEventListener("click", () => hideClusterContextMenu());
  window.addEventListener("scroll", () => hideClusterContextMenu(), true);
}

async function bootstrap() {
  requireSession();
  const url = new URL(window.location.href);
  state.platform = url.searchParams.get("platform") || "wb";
  state.windowMode = url.searchParams.get("window_mode") || "monthly";
  elements.platformSelect.value = state.platform;
  elements.windowModeSelect.value = state.windowMode;
  syncLeaderboardCopy();
  bindEvents();
  elements.heatDbPathLabel.textContent = "Loading analytics database...";
  await Promise.all([loadUpdateWindows(), loadWindows(), refreshOverviewMeta()]);
  if (isMonthlyMode() ? url.searchParams.get("month_key") : url.searchParams.get("week_start") && url.searchParams.get("week_end")) {
    const matched = state.windows.find((item) =>
      isMonthlyMode()
        ? item.month_key === url.searchParams.get("month_key")
        : item.week_start === url.searchParams.get("week_start") && item.week_end === url.searchParams.get("week_end")
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
