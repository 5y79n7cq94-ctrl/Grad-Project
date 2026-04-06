const METRIC_META = [
  { key: "discussion_total", label: "Discussion", subtitle: "Total daily discussion volume", accent: "accent-entertainment" },
  { key: "engagement_total", label: "Engagement", subtitle: "Likes, comments, and shares aggregated per day", accent: "accent-experience" },
  { key: "unique_authors", label: "Unique Authors", subtitle: "Distinct authors posting on that day", accent: "accent-shopping" },
  { key: "velocity", label: "Velocity", subtitle: "Daily post count used as the speed signal", accent: "accent-accommodation" },
];
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
  fb: "Facebook",
  wb: "Weibo",
};

const elements = {
  trendDbPathLabel: document.getElementById("trendDbPathLabel"),
  trendPlatformSelect: document.getElementById("trendPlatformSelect"),
  trendWindowModeSelect: document.getElementById("trendWindowModeSelect"),
  trendSnapshotLabel: document.getElementById("trendSnapshotLabel"),
  trendActiveWindowLabel: document.getElementById("trendActiveWindowLabel"),
  trendWeekLabel: document.getElementById("trendWeekLabel"),
  trendWeekSubLabel: document.getElementById("trendWeekSubLabel"),
  trendEventSelect: document.getElementById("trendEventSelect"),
  trendIndicatorGrid: document.getElementById("trendIndicatorGrid"),
  trendHeatFocusCard: document.getElementById("trendHeatFocusCard"),
  chartGrid: document.getElementById("chartGrid"),
  trendPageTitle: document.getElementById("trendPageTitle"),
  trendPageSubtitle: document.getElementById("trendPageSubtitle"),
  backToLeaderboardLink: document.getElementById("backToLeaderboardLink"),
  topbarLeaderboardLink: document.getElementById("topbarLeaderboardLink"),
};

const state = {
  selectedEvent: "",
  leaderboard: [],
  platform: "wb",
  windowMode: "weekly",
  currentWeek: {
    week_start: "",
    week_end: "",
    status: "",
    month_key: "",
  },
};

async function requestJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json();
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

function clipText(value, maxLength = 18) {
  const text = String(value || "").trim();
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength - 3)}...`;
}

function isMonthlyMode() {
  return state.windowMode === "monthly";
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

function formatAxisDateLabel(value) {
  if (!value) {
    return "";
  }
  const [, month, day] = String(value).split("-");
  if (!month || !day) {
    return value;
  }
  return `${Number(month)}/${Number(day)}`;
}

function getUrlState() {
  const url = new URL(window.location.href);
  return {
    event: url.searchParams.get("event") || "",
    platform: url.searchParams.get("platform") || "wb",
    windowMode: url.searchParams.get("window_mode") || (url.searchParams.get("month_key") ? "monthly" : "weekly"),
    monthKey: url.searchParams.get("month_key") || "",
    weekStart: url.searchParams.get("week_start") || "",
    weekEnd: url.searchParams.get("week_end") || "",
  };
}

function formatWeekLabel(weekStart, weekEnd) {
  if (!weekStart || !weekEnd) {
    return "No weekly snapshot selected";
  }
  return `${weekStart} to ${weekEnd}`;
}

function formatCurrentWindowLabel() {
  if (isMonthlyMode()) {
    return state.currentWeek.month_key || "No monthly snapshot selected";
  }
  return formatWeekLabel(state.currentWeek.week_start, state.currentWeek.week_end);
}

function syncLeaderboardLink() {
  const base = appendSessionParams(new URL("/full-web-heat-analysis", window.location.origin));
  if (state.selectedEvent) {
    base.searchParams.set("event", state.selectedEvent);
  }
  if (state.platform) {
    base.searchParams.set("platform", state.platform);
  }
  base.searchParams.set("window_mode", state.windowMode);
  if (isMonthlyMode() && state.currentWeek.month_key) {
    base.searchParams.set("month_key", state.currentWeek.month_key);
  } else if (state.currentWeek.week_start && state.currentWeek.week_end) {
    base.searchParams.set("week_start", state.currentWeek.week_start);
    base.searchParams.set("week_end", state.currentWeek.week_end);
  }
  elements.backToLeaderboardLink.href = `${base.pathname}${base.search}`;
  if (elements.topbarLeaderboardLink) {
    elements.topbarLeaderboardLink.href = `${base.pathname}${base.search}`;
  }
}

function syncTrendModeUI() {
  if (elements.trendSnapshotLabel) {
    elements.trendSnapshotLabel.textContent = isMonthlyMode() ? "Monthly Snapshot" : "Weekly Snapshot";
  }
  if (elements.trendActiveWindowLabel) {
    elements.trendActiveWindowLabel.textContent = isMonthlyMode() ? "Current Month" : "Current Week";
  }
  if (elements.trendPageTitle) {
    elements.trendPageTitle.textContent = isMonthlyMode() ? "Monthly Full-Web Trend Analysis" : "Weekly Full-Web Trend Analysis";
  }
  if (elements.trendPageSubtitle) {
    elements.trendPageSubtitle.textContent = isMonthlyMode()
      ? "Focus on the selected calendar month. The small indicator cards explain the drivers behind the final heat score."
      : "Focus on the current weekly snapshot only. The small indicator cards explain the drivers behind the final heat score.";
  }
}

function renderIndicatorCards(summary) {
  clearNode(elements.trendIndicatorGrid);
  const cards = [
    { label: "Discussion", value: formatNumber(summary.discussion_total || 0), sub: `Conversation volume in the selected ${isMonthlyMode() ? "month" : "week"}` },
    { label: "Engagement", value: formatNumber(summary.total_engagement || 0), sub: `Likes, comments, and shares in the selected ${isMonthlyMode() ? "month" : "week"}` },
    { label: "Unique Authors", value: formatNumber(summary.unique_authors || 0), sub: `Distinct authors in the selected ${isMonthlyMode() ? "month" : "week"}` },
    { label: "Velocity", value: formatNumber(summary.post_count || 0), sub: `Posting speed inside the selected ${isMonthlyMode() ? "monthly" : "weekly"} snapshot` },
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
  const categoryLabel = CATEGORY_LABELS[summary.dashboard_category] || "No category";
  elements.trendHeatFocusCard.innerHTML = `
    <p class="eyebrow">Final Heat</p>
    <h3>${summary.cluster_key || "-"}</h3>
    <div class="trend-heat-score">${formatScore(summary.heat_score || 0)}</div>
    <p class="helper-copy">${categoryLabel} · Final combined score for the selected ${isMonthlyMode() ? "monthly" : "weekly"} snapshot</p>
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
  if (!metricSeries.length) {
    svg.innerHTML = `<text class="metric-label" x="260" y="126" text-anchor="middle">No ${isMonthlyMode() ? "monthly" : "weekly"} data</text>`;
    return wrapper;
  }

  const width = 520;
  const height = 250;
  const padding = { top: 20, right: 16, bottom: 36, left: 42 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxValue = Math.max(1, ...metricSeries.map((item) => item.value || 0));
  const points = metricSeries.map((item, index) => {
    const x = padding.left + (chartWidth * index) / Math.max(metricSeries.length - 1, 1);
    const y = padding.top + chartHeight - ((item.value || 0) / maxValue) * chartHeight;
    return { ...item, x, y };
  });

  const linePath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const areaPath = [
    `M ${points[0].x} ${padding.top + chartHeight}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${padding.top + chartHeight}`,
    "Z",
  ].join(" ");

  const xLabels = points
    .map(
      (point) => `<text class="metric-label" x="${point.x}" y="${height - 12}" text-anchor="middle">${formatAxisDateLabel(
        point.date
      )}</text>`
    )
    .join("");

  svg.innerHTML = `
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top + chartHeight}" x2="${width - padding.right}" y2="${padding.top + chartHeight}"></line>
    <line class="metric-axis" x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${padding.top + chartHeight}"></line>
    <path class="metric-area" d="${areaPath}"></path>
    <path class="metric-line" d="${linePath}"></path>
    ${points.map((point) => `<circle class="metric-point" cx="${point.x}" cy="${point.y}" r="4.5"></circle>`).join("")}
    ${xLabels}
  `;

  return wrapper;
}

function renderCharts(metrics) {
  clearNode(elements.chartGrid);
  METRIC_META.forEach((meta) => {
    elements.chartGrid.appendChild(renderMetricChart(metrics?.[meta.key] || [], meta));
  });
}

function populateEventSelect(items) {
  clearNode(elements.trendEventSelect);
  if (!items.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No event available";
    elements.trendEventSelect.appendChild(option);
    state.selectedEvent = "";
    return;
  }

  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.cluster_key;
    option.textContent = clipText(item.cluster_key, 18);
    option.title = item.cluster_key;
    elements.trendEventSelect.appendChild(option);
  });

  const initial = getUrlState().event;
  if (initial && items.some((item) => item.cluster_key === initial)) {
    state.selectedEvent = initial;
  } else if (!items.some((item) => item.cluster_key === state.selectedEvent)) {
    state.selectedEvent = items[0].cluster_key;
  }
  elements.trendEventSelect.value = state.selectedEvent;
  syncLeaderboardLink();
}

async function loadTrendData() {
  if (!state.selectedEvent || (!isMonthlyMode() && (!state.currentWeek.week_start || !state.currentWeek.week_end)) || (isMonthlyMode() && !state.currentWeek.month_key)) {
    renderIndicatorCards({});
    renderHeatFocusCard({});
    renderCharts({});
    return;
  }

  const params = new URLSearchParams({
    platform: state.platform,
    event_family_key: state.selectedEvent,
    days: isMonthlyMode() ? "31" : "7",
  });
  if (isMonthlyMode()) {
    params.set("month_key", state.currentWeek.month_key);
  } else {
    params.set("week_start", state.currentWeek.week_start);
    params.set("week_end", state.currentWeek.week_end);
  }
  const payload = await requestJson(`/api/full-web-heat-analysis/event-trend?${params.toString()}`);
  renderIndicatorCards(payload.summary || {});
  renderHeatFocusCard(payload.summary || {});
  renderCharts(payload.metrics || {});
}

async function resolveWeeklySnapshot() {
  const urlState = getUrlState();
  const windows = await requestJson(
    `/api/full-web-heat-analysis/analysis-windows?platform=${encodeURIComponent(state.platform)}&window_mode=${encodeURIComponent(
      state.windowMode
    )}&weeks=24&periods=12`
  );
  const requestedWeek = isMonthlyMode()
    ? windows.items.find((item) => item.month_key === urlState.monthKey)
    : windows.items.find((item) => item.week_start === urlState.weekStart && item.week_end === urlState.weekEnd);
  const latestCompleted = windows.items.find((item) => item.status === "completed");
  const latestAnalyzable = windows.items.find((item) => item.status === "to_be_analyzed");
  state.currentWeek = requestedWeek || latestCompleted || latestAnalyzable || { week_start: "", week_end: "", status: "", month_key: "" };
}

async function loadPageData() {
  await resolveWeeklySnapshot();

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

  const [overview, leaderboard] = await Promise.all([
    requestJson(`/api/full-web-heat-analysis/overview?platform=${encodeURIComponent(state.platform)}&auto_sync=false`),
    requestJson(`/api/full-web-heat-analysis/event-clusters?${query.toString()}`),
  ]);

  elements.trendDbPathLabel.textContent = overview.db_path || "Unknown analytics database";
  if (overview.total_posts === 0) {
    elements.trendWeekLabel.textContent = "Database is empty";
    elements.trendWeekSubLabel.textContent = "No social posts were found in the configured analytics database yet.";
  } else if ((!isMonthlyMode() && !state.currentWeek.week_start) || (isMonthlyMode() && !state.currentWeek.month_key)) {
    elements.trendWeekLabel.textContent = isMonthlyMode() ? "No analyzed month yet" : "No analyzed week yet";
    elements.trendWeekSubLabel.textContent = `Open Heat Analysis and run one ${isMonthlyMode() ? "monthly" : "weekly"} analysis first.`;
  } else {
    elements.trendWeekLabel.textContent = formatCurrentWindowLabel();
    elements.trendWeekSubLabel.textContent =
      state.currentWeek.status === "completed"
        ? `Trend charts are limited to the selected ${isMonthlyMode() ? "calendar month" : "Sunday to Saturday snapshot"}.`
        : `This ${isMonthlyMode() ? "month" : "week"} has raw posts but no analyzed clusters yet. Run analysis from Heat Analysis first.`;
  }

  state.leaderboard = leaderboard.items || [];
  populateEventSelect(state.leaderboard);
  syncLeaderboardLink();
  await loadTrendData();
}

async function bootstrap() {
  enforceSystemSession();
  const urlState = getUrlState();
  state.platform = urlState.platform || "wb";
  state.windowMode = urlState.windowMode || "weekly";
  elements.trendPlatformSelect.value = state.platform;
  if (elements.trendWindowModeSelect) {
    elements.trendWindowModeSelect.value = state.windowMode;
  }
  syncTrendModeUI();

  elements.trendPlatformSelect.addEventListener("change", async (event) => {
    state.platform = event.target.value || "wb";
    state.selectedEvent = "";
    await loadPageData();
  });

  if (elements.trendWindowModeSelect) {
    elements.trendWindowModeSelect.addEventListener("change", async (event) => {
      state.windowMode = event.target.value || "weekly";
      syncTrendModeUI();
      state.selectedEvent = "";
      await loadPageData();
    });
  }

  elements.trendEventSelect.addEventListener("change", async (event) => {
    state.selectedEvent = event.target.value;
    syncLeaderboardLink();
    await loadTrendData();
  });

  await loadPageData();
}

bootstrap().catch((error) => {
  elements.trendDbPathLabel.textContent = `Load failed: ${error.message}`;
});
