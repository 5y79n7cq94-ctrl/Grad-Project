const METRIC_META = [
  { key: "discussion_total", label: "Discussion", subtitle: "Total daily discussion volume", accent: "accent-entertainment" },
  { key: "engagement_total", label: "Engagement", subtitle: "Likes, comments, and shares aggregated per day", accent: "accent-experience" },
  { key: "unique_authors", label: "Unique Authors", subtitle: "Distinct authors posting on that day", accent: "accent-shopping" },
  { key: "velocity", label: "Velocity", subtitle: "Daily post count used as the speed signal", accent: "accent-accommodation" },
];
const API_BASE = "/api/full-web-heat-analysis";
const HEAT_ROUTE = "/full-web-heat-analysis";
const MARKET_ROUTE = "/operation_panel.html";
const LOGIN_ROUTE = "/login_page.html";
const SESSION_KEYS = ["first_name", "last_name", "email", "position", "role", "token"];

const PLATFORM_LABELS = {
  fb: "Facebook",
  wb: "Weibo",
};

const STATUS_META = {
  completed: {
    label: "Completed",
    className: "status-completed",
    detail: "This snapshot already has analyzed clusters.",
  },
  to_be_analyzed: {
    label: "To Be Analyzed",
    className: "status-updated",
    detail: "Raw posts already exist, but cluster analysis still needs to run.",
  },
  to_be_updated: {
    label: "To Be Updated",
    className: "status-available",
    detail: "No raw posts exist in this window yet.",
  },
  future: {
    label: "Future",
    className: "status-future",
    detail: "Future windows cannot be selected yet.",
  },
};

const elements = {
  trendDbPathLabel: document.getElementById("trendDbPathLabel"),
  trendPlatformSelect: document.getElementById("trendPlatformSelect"),
  trendWindowModeSelect: document.getElementById("trendWindowModeSelect"),
  trendWindowEyebrow: document.getElementById("trendWindowEyebrow"),
  trendWeekLabel: document.getElementById("trendWeekLabel"),
  trendWeekSubLabel: document.getElementById("trendWeekSubLabel"),
  trendSnapshotFilterLabel: document.getElementById("trendSnapshotFilterLabel"),
  trendSnapshotFilterCopy: document.getElementById("trendSnapshotFilterCopy"),
  openTrendCalendarButton: document.getElementById("openTrendCalendarButton"),
  trendSnapshotWindowList: document.getElementById("trendSnapshotWindowList"),
  trendEventSelect: document.getElementById("trendEventSelect"),
  trendIndicatorGrid: document.getElementById("trendIndicatorGrid"),
  trendHeatFocusCard: document.getElementById("trendHeatFocusCard"),
  chartGrid: document.getElementById("chartGrid"),
  backToLeaderboardLink: document.getElementById("backToLeaderboardLink"),
  trendBackToMarketLink: document.getElementById("trendBackToMarketLink"),
  trendPageTitle: document.getElementById("trendPageTitle"),
  trendPageSubtitlePrefix: document.getElementById("trendPageSubtitlePrefix"),
  trendBusyOverlay: document.getElementById("trendBusyOverlay"),
  trendBusyTitle: document.getElementById("trendBusyTitle"),
  trendBusyDetail: document.getElementById("trendBusyDetail"),
  trendCalendarModal: document.getElementById("trendCalendarModal"),
  trendCalendarBackdrop: document.getElementById("trendCalendarBackdrop"),
  closeTrendCalendarButton: document.getElementById("closeTrendCalendarButton"),
  trendCalendarMonthLabel: document.getElementById("trendCalendarMonthLabel"),
  trendCalendarGrid: document.getElementById("trendCalendarGrid"),
  trendCalendarEyebrow: document.getElementById("trendCalendarEyebrow"),
  trendCalendarTitle: document.getElementById("trendCalendarTitle"),
  trendCalendarHelper: document.getElementById("trendCalendarHelper"),
  trendCalendarSelectionLabel: document.getElementById("trendCalendarSelectionLabel"),
  trendCalendarSelectionDetail: document.getElementById("trendCalendarSelectionDetail"),
  trendCalendarLegend: document.getElementById("trendCalendarLegend"),
  confirmTrendCalendarButton: document.getElementById("confirmTrendCalendarButton"),
};

const state = {
  selectedEvent: "",
  leaderboard: [],
  platform: "wb",
  windowMode: "monthly",
  useInitialUrlEvent: true,
  windows: [],
  calendarSelectedWindow: null,
  currentWeek: {
    week_start: "",
    week_end: "",
    month_key: "",
    status: "",
  },
  overviewCache: {},
};

const QUARTERLY_PENDING_COPY =
  "Quarterly reporting is not available yet. Full-Web collection started on 2026-03-01, and the first complete Q2 2026 report will be available after June 2026.";

async function requestJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
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

function formatAxisDateLabel(value) {
  if (!value) {
    return "";
  }
  const [, month, day] = String(value).split("-");
  if (!month || !day) {
    return value;
  }
  if (isMonthlyMode()) {
    return `${month}/${day}`;
  }
  return `${Number(month)}/${Number(day)}`;
}

function getAxisLabelIndexes(length) {
  if (length <= 0) {
    return new Set();
  }
  const targetTickCount = isMonthlyMode() ? 6 : 7;
  const step = Math.max(1, Math.ceil(length / targetTickCount));
  const indexes = new Set([0, length - 1]);
  for (let index = 0; index < length; index += step) {
    indexes.add(index);
  }
  return indexes;
}

function buildLinePath(points) {
  if (!points.length) {
    return "";
  }
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
}

function buildAreaPath(points, baselineY) {
  if (!points.length) {
    return "";
  }
  return [
    `M ${points[0].x} ${baselineY}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${baselineY}`,
    "Z",
  ].join(" ");
}

function clipOptionLabel(value, maxLength = 34) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 3)}...`;
}

function isMonthlyMode() {
  return state.windowMode === "monthly";
}

function isQuarterlyMode() {
  return state.windowMode === "quarterly";
}

function setPanelBusy(isBusy, title = "Data is loading...", detail = "Please wait while the trend charts refresh.") {
  elements.trendBusyOverlay?.classList.toggle("hidden", !isBusy);
  elements.trendBusyOverlay?.setAttribute("aria-hidden", isBusy ? "false" : "true");
  if (elements.trendBusyTitle) {
    elements.trendBusyTitle.textContent = title;
  }
  if (elements.trendBusyDetail) {
    elements.trendBusyDetail.textContent = detail;
  }
}

function getUrlState() {
  const url = new URL(window.location.href);
  return {
    event: url.searchParams.get("event") || "",
    platform: url.searchParams.get("platform") || "wb",
    windowMode: url.searchParams.get("window_mode") || "monthly",
    weekStart: url.searchParams.get("week_start") || "",
    weekEnd: url.searchParams.get("week_end") || "",
    monthKey: url.searchParams.get("month_key") || "",
  };
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No weekly snapshot selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function formatMonthLabel(monthKey) {
  if (!monthKey) {
    return "No monthly snapshot selected";
  }
  return monthKey;
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
  if (isMonthlyMode()) {
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

function syncLeaderboardLink() {
  elements.backToLeaderboardLink.href = buildUrlWithSession(HEAT_ROUTE, {
    event: state.selectedEvent,
    platform: state.platform,
    window_mode: state.windowMode,
    month_key: isMonthlyMode() ? state.currentWeek.month_key : "",
    week_start: !isQuarterlyMode() ? state.currentWeek.week_start : "",
    week_end: !isQuarterlyMode() ? state.currentWeek.week_end : "",
  });
  if (elements.trendBackToMarketLink) {
    elements.trendBackToMarketLink.href = buildUrlWithSession(MARKET_ROUTE);
  }
}

function renderSnapshotWindowList() {
  clearNode(elements.trendSnapshotWindowList);
  if (isQuarterlyMode()) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent =
      "Quarterly reporting is not available yet. Full-Web collection starts on 2026-03-01, so the first complete Q2 2026 report will be available after June 2026.";
    elements.trendSnapshotWindowList.appendChild(empty);
    return;
  }
  if (!state.windows.length) {
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent = isMonthlyMode()
      ? "No monthly windows were found for the selected platform."
      : "No weekly windows were found for the selected platform.";
    elements.trendSnapshotWindowList.appendChild(empty);
    return;
  }

  state.windows.forEach((item) => {
    const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
    const isSelected = isMonthlyMode()
      ? state.currentWeek?.month_key === item.month_key
      : state.currentWeek?.week_start === item.week_start && state.currentWeek?.week_end === item.week_end;
    const button = document.createElement("button");
    button.type = "button";
    button.className = `snapshot-window-card ${statusMeta.className}${isSelected ? " selected" : ""}`;
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
      state.currentWeek = item;
      renderSnapshotWindowList();
      syncTrendWindowCopy();
      await loadPageData();
    });
    elements.trendSnapshotWindowList.appendChild(button);
  });
}

function syncTrendWindowCopy() {
  elements.trendWindowEyebrow.textContent = isQuarterlyMode() ? "Current Quarter" : isMonthlyMode() ? "Current Month" : "Current Week";
  elements.trendSnapshotFilterLabel.textContent = isQuarterlyMode() ? "Quarterly Filter" : isMonthlyMode() ? "Monthly Filter" : "Weekly Filter";
  elements.trendSnapshotFilterCopy.textContent = isQuarterlyMode()
    ? "Quarterly reporting is visible for planning, but Full-Web collection starts on 2026-03-01. The first complete Q2 2026 report will be available after June 2026."
    : isMonthlyMode()
    ? "Monthly snapshots are derived from your existing Full-Web database. Pick Month to switch between available monthly cluster windows."
    : "Fixed Sunday to Saturday windows only. Pick Week to switch between completed, analyzable, or future weekly snapshots.";
  elements.openTrendCalendarButton.textContent = isQuarterlyMode() ? "Pick Quarter" : isMonthlyMode() ? "Pick Month" : "Pick Week";
  if (isQuarterlyMode()) {
    elements.trendWeekLabel.textContent = state.currentWeek?.quarter_key || "2026-Q2";
    elements.trendWeekSubLabel.textContent =
      "Quarterly reporting is pending until a complete Q2 2026 window is available after June 2026.";
    return;
  }
  if ((!isMonthlyMode() && !state.currentWeek.week_start) || (isMonthlyMode() && !state.currentWeek.month_key)) {
    elements.trendWeekLabel.textContent = isMonthlyMode() ? "No analyzed month yet" : "No analyzed week yet";
    elements.trendWeekSubLabel.textContent = `Open Heat Analysis and run one ${isMonthlyMode() ? "monthly" : "weekly"} analysis first.`;
    return;
  }
  const statusMeta = STATUS_META[state.currentWeek.status] || STATUS_META.to_be_updated;
  elements.trendWeekLabel.textContent = formatSelectedWindowLabel(state.currentWeek);
  elements.trendWeekSubLabel.textContent = `${statusMeta.label}. ${statusMeta.detail}`;
}

function renderTrendCalendarLegend() {
  clearNode(elements.trendCalendarLegend);
  [
    { label: "To Be Updated", className: "swatch-available" },
    { label: "To Be Analyzed", className: "swatch-updated" },
    { label: "Completed", className: "swatch-imported" },
    { label: "Future", className: "swatch-future" },
  ].forEach((item) => {
    const node = document.createElement("span");
    node.className = "calendar-legend-item";
    node.innerHTML = `<span class="calendar-swatch ${item.className}"></span>${item.label}`;
    elements.trendCalendarLegend.appendChild(node);
  });
}

function syncTrendCalendarSelectionSummary() {
  elements.trendCalendarSelectionLabel.textContent = state.calendarSelectedWindow
    ? formatSelectedWindowLabel(state.calendarSelectedWindow)
    : (isMonthlyMode() ? "No month selected" : "No week selected");
  elements.trendCalendarSelectionDetail.textContent = state.calendarSelectedWindow
    ? `${(STATUS_META[state.calendarSelectedWindow.status] || STATUS_META.to_be_updated).label}. ${
        (STATUS_META[state.calendarSelectedWindow.status] || STATUS_META.to_be_updated).detail
      }`
    : `Choose one ${isMonthlyMode() ? "monthly" : "weekly"} snapshot to refresh trend charts.`;
  elements.confirmTrendCalendarButton.disabled = !state.calendarSelectedWindow;
  elements.confirmTrendCalendarButton.textContent = isMonthlyMode() ? "Use This Month" : "Use This Week";
}

function renderTrendCalendar() {
  clearNode(elements.trendCalendarGrid);
  elements.trendCalendarGrid.classList.add("calendar-scroll-grid");
  if (isQuarterlyMode()) {
    elements.trendCalendarMonthLabel.textContent = "Quarterly Reporting";
    elements.trendCalendarEyebrow.textContent = "Quarterly Trend Filter";
    elements.trendCalendarTitle.textContent = "Pick Quarter";
    elements.trendCalendarHelper.textContent =
      "Quarterly reporting is visible for planning, but a complete quarter is not available yet. The first complete Q2 2026 report will be available after June 2026.";
    const empty = document.createElement("div");
    empty.className = "snapshot-window-empty";
    empty.textContent =
      "Full-Web collection only starts on 2026-03-01. Please wait until after June 2026 for the first complete Q2 2026 quarterly report.";
    elements.trendCalendarGrid.appendChild(empty);
    clearNode(elements.trendCalendarLegend);
    elements.trendCalendarSelectionLabel.textContent = "2026-Q2";
    elements.trendCalendarSelectionDetail.textContent =
      "Quarterly reporting is pending until a complete Q2 2026 window is available.";
    elements.confirmTrendCalendarButton.disabled = true;
    elements.confirmTrendCalendarButton.textContent = "Use This Quarter";
    return;
  }
  const monthKeys = getWindowMonthKeys(state.windows);
  elements.trendCalendarMonthLabel.textContent = isMonthlyMode() ? "Available Months" : "Available Weeks";
  elements.trendCalendarEyebrow.textContent = isMonthlyMode() ? "Monthly Trend Filter" : "Weekly Trend Filter";
  elements.trendCalendarTitle.textContent = isMonthlyMode() ? "Pick Month" : "Pick Week";
  elements.trendCalendarHelper.textContent = isMonthlyMode()
    ? "Choose one calendar month for the trend view. The time selector matches the leaderboard snapshot logic."
    : "Choose one fixed Sunday to Saturday window for the trend view. The time selector matches the leaderboard snapshot logic.";

  if (isMonthlyMode()) {
    const section = document.createElement("section");
    section.className = "calendar-month-section";
    section.innerHTML = `<h3 class="calendar-section-title">Calendar Months</h3>`;
    const grid = document.createElement("div");
    grid.className = "calendar-section-grid calendar-section-grid-months";
    state.windows.forEach((item) => {
      const statusMeta = STATUS_META[item.status] || STATUS_META.to_be_updated;
      const isSelected = state.calendarSelectedWindow?.month_key === item.month_key;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
      button.innerHTML = `
        <span class="calendar-day-month">${formatMonthLabel(item.month_key)}</span>
        <span class="calendar-day-meta">${formatNumber(item.post_count || 0)} posts · calendar month window</span>
      `;
      button.addEventListener("click", () => {
        state.calendarSelectedWindow = item;
        syncTrendCalendarSelectionSummary();
        renderTrendCalendar();
      });
      grid.appendChild(button);
    });
    section.appendChild(grid);
    elements.trendCalendarGrid.appendChild(section);
  } else {
    monthKeys.forEach((monthKey) => {
      const visibleWeeks = state.windows.filter((item) => monthOverlapsWindow(monthKey, item));
      if (!visibleWeeks.length) {
        return;
      }
      const section = document.createElement("section");
      section.className = "calendar-month-section";
      section.innerHTML = `<h3 class="calendar-section-title">${formatMonthHeading(monthKey)}</h3>`;
      const grid = document.createElement("div");
      grid.className = "calendar-section-grid";
      visibleWeeks.forEach((week) => {
        const statusMeta = STATUS_META[week.status] || STATUS_META.to_be_updated;
        const isSelected =
          state.calendarSelectedWindow?.week_start === week.week_start &&
          state.calendarSelectedWindow?.week_end === week.week_end;
        const button = document.createElement("button");
        button.type = "button";
        button.className = `calendar-day ${statusMeta.className}${isSelected ? " selected-week" : ""}`;
        button.innerHTML = `
          <span class="calendar-week-range">${week.week_start} to ${week.week_end}</span>
          <span class="calendar-day-meta">${formatNumber(week.post_count || 0)} posts · fixed Sunday-Saturday window</span>
        `;
        button.addEventListener("click", () => {
          state.calendarSelectedWindow = week;
          syncTrendCalendarSelectionSummary();
          renderTrendCalendar();
        });
        grid.appendChild(button);
      });
      section.appendChild(grid);
      elements.trendCalendarGrid.appendChild(section);
    });
  }

  renderTrendCalendarLegend();
  syncTrendCalendarSelectionSummary();
}

function openTrendCalendar() {
  state.calendarSelectedWindow = state.currentWeek;
  renderTrendCalendar();
  elements.trendCalendarModal.classList.remove("hidden");
  elements.trendCalendarModal.setAttribute("aria-hidden", "false");
}

function closeTrendCalendar() {
  elements.trendCalendarModal.classList.add("hidden");
  elements.trendCalendarModal.setAttribute("aria-hidden", "true");
}

function renderIndicatorCards(summary) {
  clearNode(elements.trendIndicatorGrid);
  if (isQuarterlyMode()) {
    const cards = [
      {
        label: "Quarterly Status",
        value: "Not Available Yet",
        sub: "Quarterly reports require one complete calendar quarter of data.",
        wide: true,
      },
      {
        label: "Collection Start",
        value: "2026-03-01",
        sub: "Full-Web collection began in March 2026.",
      },
      {
        label: "First Report",
        value: "2026-Q2",
        sub: "The first complete quarterly report becomes available after June 2026.",
      },
      {
        label: "Scope",
        value: PLATFORM_LABELS[state.platform] || "Platform",
        sub: "Quarterly planning view for the selected platform.",
      },
    ];
    cards.forEach((card) => {
      const node = document.createElement("article");
      node.className = `trend-indicator-card${card.wide ? " trend-indicator-card-wide" : ""}`;
      node.innerHTML = `
        <span>${card.label}</span>
        <strong>${card.value}</strong>
        <small>${card.sub}</small>
      `;
      elements.trendIndicatorGrid.appendChild(node);
    });
    return;
  }
  const cards = [
    {
      label: "Discussion",
      value: formatNumber(summary.discussion_total || 0),
      sub: `Conversation volume in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Engagement",
      value: formatNumber(summary.total_engagement || 0),
      sub: `Likes, comments, and shares in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Unique Authors",
      value: formatNumber(summary.unique_authors || 0),
      sub: `Distinct authors in the selected ${isMonthlyMode() ? "month" : "week"}`,
    },
    {
      label: "Velocity",
      value: formatNumber(summary.post_count || 0),
      sub: `Posting speed inside the selected ${isMonthlyMode() ? "monthly" : "weekly"} snapshot`,
    },
  ];

  cards.forEach((card) => {
    const node = document.createElement("article");
    node.className = "trend-indicator-card";
    node.innerHTML = `
      <span>${card.label}</span>
      <strong>${card.value}</strong>
      <small>${card.sub}</small>
    `;
    elements.trendIndicatorGrid.appendChild(node);
  });
}

function renderHeatFocusCard(summary) {
  if (isQuarterlyMode()) {
    elements.trendHeatFocusCard.classList.add("pending");
    elements.trendHeatFocusCard.innerHTML = `
      <p class="eyebrow">Quarterly Status</p>
      <span class="quarterly-status-pill">Not Available Yet</span>
      <h3>Quarterly report pending</h3>
      <div class="trend-heat-score">--</div>
      <p class="helper-copy">${QUARTERLY_PENDING_COPY}</p>
    `;
    return;
  }
  elements.trendHeatFocusCard.classList.remove("pending");
  elements.trendHeatFocusCard.innerHTML = `
    <p class="eyebrow">Final Heat</p>
    <h3>${summary.cluster_key || "-"}</h3>
    <div class="trend-heat-score">${formatScore(summary.heat_score || 0)}</div>
    <p class="helper-copy">Final combined score for the selected ${isQuarterlyMode() ? "quarterly" : isMonthlyMode() ? "monthly" : "weekly"} snapshot</p>
  `;
}

function renderMetricChart(metricSeries, meta) {
  const wrapper = document.createElement("article");
  wrapper.className = "chart-card";
  wrapper.innerHTML = `
    <div class="chart-card-head">
      <div>
        <p class="chart-kicker">${meta.label}</p>
        <h3>${meta.subtitle}</h3>
      </div>
      <span class="pill-badge ${meta.accent}">${meta.label}</span>
    </div>
    <div class="chart-stage">
      <svg class="metric-chart" viewBox="0 0 520 250" preserveAspectRatio="none"></svg>
    </div>
  `;

  const svg = wrapper.querySelector("svg");
  const validSeries = metricSeries.filter((item) => item.value !== null && item.value !== undefined);
  if (!validSeries.length) {
    svg.innerHTML = `<text class="metric-label" x="260" y="126" text-anchor="middle">No ${isMonthlyMode() ? "monthly" : "weekly"} data</text>`;
    return wrapper;
  }

  const width = 520;
  const height = 250;
  const padding = { top: 20, right: 16, bottom: 36, left: 42 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxValue = Math.max(1, ...validSeries.map((item) => Number(item.value || 0)));
  const points = metricSeries.map((item, index) => {
    const x = padding.left + (chartWidth * index) / Math.max(metricSeries.length - 1, 1);
    const hasValue = item.value !== null && item.value !== undefined;
    const y = hasValue ? padding.top + chartHeight - ((item.value || 0) / maxValue) * chartHeight : null;
    return { ...item, x, y, hasValue };
  });
  const plottedPoints = points.filter((point) => point.hasValue);
  const linePath = buildLinePath(plottedPoints);
  const areaPath = buildAreaPath(plottedPoints, padding.top + chartHeight);
  const axisLabelIndexes = getAxisLabelIndexes(metricSeries.length);
  const xLabels = points
    .filter((point, index) => axisLabelIndexes.has(index))
    .map(
      (point) => `<text class="metric-label" x="${point.x}" y="${height - 12}" text-anchor="middle">${formatAxisDateLabel(
        point.date
      )}</text>`
    )
    .join("");
  const trailingMissingIndex = points.findIndex((point, index) => !point.hasValue && index > 0);
  const pendingLabel = trailingMissingIndex >= 0
    ? `<text class="metric-pending-label" x="${width - padding.right}" y="${padding.top + 16}" text-anchor="end">Awaiting later data</text>`
    : "";

  svg.innerHTML = `
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top + chartHeight}" x2="${width - padding.right}" y2="${padding.top + chartHeight}"></line>
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${padding.top + chartHeight}"></line>
    ${areaPath ? `<path class="metric-area" d="${areaPath}"></path>` : ""}
    ${linePath ? `<path class="metric-line" d="${linePath}"></path>` : ""}
    ${plottedPoints.map((point) => `<circle class="metric-point" cx="${point.x}" cy="${point.y}" r="4.5"></circle>`).join("")}
    ${xLabels}
    ${pendingLabel}
  `;

  return wrapper;
}

function renderCharts(metrics) {
  clearNode(elements.chartGrid);
  if (isQuarterlyMode()) {
    METRIC_META.forEach((meta) => {
      const wrapper = document.createElement("article");
      wrapper.className = "chart-card quarterly-empty-card";
      wrapper.innerHTML = `
        <div class="chart-card-head">
          <div>
            <p class="chart-kicker">${meta.label}</p>
            <h3>${meta.subtitle}</h3>
          </div>
          <span class="pill-badge ${meta.accent}">${meta.label}</span>
        </div>
        <div>
          <h4>Quarterly report not available</h4>
          <p>${QUARTERLY_PENDING_COPY}</p>
        </div>
      `;
      elements.chartGrid.appendChild(wrapper);
    });
    return;
  }
  METRIC_META.forEach((meta) => {
    elements.chartGrid.appendChild(renderMetricChart(metrics?.[meta.key] || [], meta));
  });
}

function populateEventSelect(items) {
  clearNode(elements.trendEventSelect);
  if (!items.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = isQuarterlyMode() ? "Quarterly report not available" : "No event available";
    elements.trendEventSelect.appendChild(option);
    state.selectedEvent = "";
    elements.trendEventSelect.disabled = isQuarterlyMode();
    return;
  }

  elements.trendEventSelect.disabled = false;
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.cluster_key;
    option.textContent = clipOptionLabel(item.cluster_key);
    option.title = item.cluster_key;
    elements.trendEventSelect.appendChild(option);
  });

  const initial = state.useInitialUrlEvent ? getUrlState().event : "";
  if (initial && items.some((item) => item.cluster_key === initial)) {
    state.selectedEvent = initial;
  } else if (!items.some((item) => item.cluster_key === state.selectedEvent)) {
    state.selectedEvent = items[0].cluster_key;
  }
  elements.trendEventSelect.value = state.selectedEvent;
  state.useInitialUrlEvent = false;
  syncLeaderboardLink();
}

async function loadTrendData() {
  setPanelBusy(true, "Data is loading...", "Refreshing the trend charts for the selected platform, window, and event.");
  if (isQuarterlyMode()) {
    renderIndicatorCards({});
    renderHeatFocusCard({});
    renderCharts({});
    setPanelBusy(false);
    return;
  }
  if (
    !state.selectedEvent ||
    (!isMonthlyMode() && (!state.currentWeek.week_start || !state.currentWeek.week_end)) ||
    (isMonthlyMode() && !state.currentWeek.month_key)
  ) {
    renderIndicatorCards({});
    renderHeatFocusCard({});
    renderCharts({});
    setPanelBusy(false);
    return;
  }

  const params = new URLSearchParams({
    platform: state.platform,
    event_family_key: state.selectedEvent,
    days: "7",
    window_mode: state.windowMode,
  });
  if (isMonthlyMode()) {
    params.set("month_key", state.currentWeek.month_key);
  } else {
    params.set("week_start", state.currentWeek.week_start);
    params.set("week_end", state.currentWeek.week_end);
  }
  try {
    const payload = await requestJson(`${API_BASE}/event-trend?${params.toString()}`);
    renderIndicatorCards(payload.summary || {});
    renderHeatFocusCard(payload.summary || {});
    renderCharts(payload.metrics || {});
  } finally {
    setPanelBusy(false);
  }
}

function resolveSnapshot() {
  const urlState = getUrlState();
  const requestedWeek = state.windows.find((item) =>
    isMonthlyMode()
      ? item.month_key === urlState.monthKey
      : item.week_start === urlState.weekStart && item.week_end === urlState.weekEnd
  );
  const currentMatched = state.windows.find((item) =>
    isMonthlyMode()
      ? item.month_key === state.currentWeek.month_key
      : item.week_start === state.currentWeek.week_start && item.week_end === state.currentWeek.week_end
  );
  const latestCompleted = state.windows.find((item) => item.status === "completed");
  const latestAnalyzable = state.windows.find((item) => item.status === "to_be_analyzed");
  const latestUpdate = state.windows.find((item) => item.status === "to_be_updated");
  state.currentWeek =
    currentMatched || requestedWeek || latestCompleted || latestAnalyzable || latestUpdate || { week_start: "", week_end: "", month_key: "", status: "" };
}

async function loadWindows() {
  const payload = await requestJson(
    `${API_BASE}/analysis-windows?platform=${encodeURIComponent(state.platform)}&weeks=24&window_mode=${encodeURIComponent(
      state.windowMode
    )}`
  );
  state.windows = payload.items || [];
  state.quarterlyMessage = payload.message || "";
  resolveSnapshot();
  syncTrendWindowCopy();
  renderSnapshotWindowList();
}

async function refreshOverviewMeta(force = false) {
  if (!force && state.overviewCache[state.platform]) {
    const cached = state.overviewCache[state.platform];
    elements.trendDbPathLabel.textContent = cached.db_path || "Unknown analytics database";
    return cached;
  }
  const overview = await requestJson(`${API_BASE}/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`);
  state.overviewCache[state.platform] = overview;
  elements.trendDbPathLabel.textContent = overview.db_path || "Unknown analytics database";
  return overview;
}

async function loadPageData() {
  setPanelBusy(true, "Data is loading...", "Refreshing the trend page for the current filters.");
  try {
    const query = new URLSearchParams({
      platform: state.platform,
      limit: "60",
    });
    if (isMonthlyMode() && state.currentWeek.month_key) {
      query.set("month_key", state.currentWeek.month_key);
    } else if (state.currentWeek.week_start && state.currentWeek.week_end) {
      query.set("week_start", state.currentWeek.week_start);
      query.set("week_end", state.currentWeek.week_end);
    }

    elements.trendDbPathLabel.textContent = state.overviewCache[state.platform]?.db_path || "Loading analytics database...";
    const [overview, leaderboard] = await Promise.all([
      refreshOverviewMeta(),
      isQuarterlyMode() ? Promise.resolve({ items: [] }) : requestJson(`${API_BASE}/event-clusters?${query.toString()}`),
    ]);

    elements.trendPageTitle.textContent = isQuarterlyMode()
      ? "Quarterly Trend Analysis"
      : isMonthlyMode()
      ? "Monthly Trend Analysis"
      : "Weekly Trend Analysis";
    elements.trendPageSubtitlePrefix.textContent = isQuarterlyMode()
      ? "Quarterly reporting is shown as a planning state until one complete quarter is available."
      : isMonthlyMode()
      ? "Focus on the current monthly snapshot only."
      : "Focus on the current weekly snapshot only.";
    if (overview.total_posts === 0) {
      elements.trendWeekLabel.textContent = "Database is empty";
      elements.trendWeekSubLabel.textContent = "No social posts were found in social_media_analytics.db.";
    } else {
      syncTrendWindowCopy();
    }

    state.leaderboard = leaderboard.items || [];
    populateEventSelect(state.leaderboard);
    syncLeaderboardLink();
    await loadTrendData();
  } catch (error) {
    setPanelBusy(false);
    throw error;
  }
}

async function bootstrap() {
  requireSession();
  const urlState = getUrlState();
  state.platform = urlState.platform || "wb";
  state.windowMode = urlState.windowMode || "monthly";
  elements.trendPlatformSelect.value = state.platform;
  elements.trendWindowModeSelect.value = state.windowMode;
  elements.trendDbPathLabel.textContent = "Loading analytics database...";

  elements.trendPlatformSelect.addEventListener("change", async (event) => {
    state.platform = event.target.value || "wb";
    state.selectedEvent = "";
    state.useInitialUrlEvent = false;
    state.currentWeek = { week_start: "", week_end: "", month_key: "", status: "" };
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await loadPageData();
  });

  elements.trendWindowModeSelect.addEventListener("change", async (event) => {
    state.windowMode = event.target.value || "monthly";
    state.selectedEvent = "";
    state.useInitialUrlEvent = false;
    state.currentWeek = { week_start: "", week_end: "", month_key: "", status: "" };
    await Promise.all([loadWindows(), refreshOverviewMeta(true)]);
    await loadPageData();
  });

  elements.trendEventSelect.addEventListener("change", async (event) => {
    state.selectedEvent = event.target.value;
    syncLeaderboardLink();
    await loadTrendData();
  });

  elements.openTrendCalendarButton.addEventListener("click", openTrendCalendar);
  elements.closeTrendCalendarButton.addEventListener("click", closeTrendCalendar);
  elements.trendCalendarBackdrop.addEventListener("click", closeTrendCalendar);
  elements.confirmTrendCalendarButton.addEventListener("click", async () => {
    if (!state.calendarSelectedWindow) {
      return;
    }
    state.currentWeek = state.calendarSelectedWindow;
    renderSnapshotWindowList();
    syncTrendWindowCopy();
    closeTrendCalendar();
    await loadPageData();
  });

  await Promise.all([loadWindows(), refreshOverviewMeta()]);
  await loadPageData();
}

bootstrap().catch((error) => {
  elements.trendDbPathLabel.textContent = `Load failed: ${error.message}`;
});
