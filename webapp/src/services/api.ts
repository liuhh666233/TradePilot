const API_BASE = "/api";

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

export type WorkflowPhase = "pre_market" | "post_market";
export type InsightState = "not_requested" | "pending" | "completed" | "failed" | "stale";
export type InsightSectionKey =
  | "market_view"
  | "theme_view"
  | "position_view"
  | "tomorrow_view"
  | "action_frame"
  | "risk_notes"
  | "execution_notes"
  | "custom";

export interface WorkflowStepResult {
  name: string;
  status: string;
  records_affected: number;
  error_message?: string | null;
}

export interface WorkflowRunSummary {
  title: string;
  overview: string;
  watch_context?: {
    watch_sectors?: string[];
    watch_stocks?: Array<{ code: string; name?: string }>;
    open_positions?: Array<{ code: string; name?: string }>;
  };
  alerts?: any[];
  steps?: WorkflowStepResult[];
}

export interface WorkflowRun {
  id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  triggered_by: string;
  status: string;
  started_at: string;
  finished_at?: string | null;
  summary: WorkflowRunSummary;
  error_message?: string | null;
}

export interface WorkflowRunResponse {
  run: WorkflowRun;
}

export interface WorkflowContextPayload {
  schema_version: string;
  producer: string;
  producer_version: string;
  generated_at: string;
  workflow_run_id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  context: Record<string, any>;
  metadata: Record<string, any>;
}

export interface InsightMetric {
  label: string;
  value: string | number | null;
}

export interface InsightListItem {
  title?: string;
  description?: string;
  status?: string;
  tags?: string[];
}

export interface WorkflowInsightSection {
  key: InsightSectionKey | string;
  title: string;
  summary?: string;
  bullets?: string[];
  tags?: string[];
  metrics?: InsightMetric[];
  items?: InsightListItem[];
}

export interface WorkflowInsightPayload {
  summary?: string;
  sections?: WorkflowInsightSection[];
}

export interface WorkflowInsightRecord {
  id: number;
  workflow_run_id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  producer: string;
  status: InsightState;
  schema_version: string;
  producer_version: string;
  generated_at: string;
  source_run_id: number;
  source_context_schema_version: string;
  insight: WorkflowInsightPayload;
  error_message?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface WorkflowInsightResponse {
  insight: WorkflowInsightRecord | null;
  state: InsightState;
  is_stale: boolean;
  latest_run_id: number | null;
}

export interface WorkflowInsightUpsertRequest {
  workflow_date: string;
  phase: WorkflowPhase;
  producer?: string;
  status?: InsightState;
  schema_version?: string;
  producer_version: string;
  generated_at: string;
  source_run_id: number;
  source_context_schema_version?: string;
  insight: WorkflowInsightPayload;
  error_message?: string | null;
}

// Portfolio
export const getPositions = () => fetchJson<any[]>("/portfolio/positions");
export const addPosition = (data: any) => fetch(`${API_BASE}/portfolio/positions`, { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const getTrades = () => fetchJson<any[]>("/portfolio/trades");

// Briefing
export const getAlerts = (unreadOnly = false) => fetchJson<any[]>(`/briefing/alerts${unreadOnly ? "?unread_only=true" : ""}`);
export const markAlertRead = (id: number) => fetch(`${API_BASE}/briefing/alerts/${id}/read`, { method: "POST" }).then(r => r.json());

// Scheduler
export const getSchedulerStatus = () => fetchJson<any>("/scheduler/status");
export const getSchedulerHistory = (limit = 10) => fetchJson<any[]>(`/scheduler/history?limit=${limit}`);

// Workflow
export const getWorkflowStatus = () => fetchJson<any>("/workflow/status");
export const getWorkflowHistory = (limit = 10) => fetchJson<any[]>(`/workflow/history?limit=${limit}`);
export const getLatestWorkflow = (phase: WorkflowPhase) =>
  fetchJson<WorkflowRunResponse | null>(`/workflow/latest?phase=${phase}`);
export const getLatestWorkflowContext = (phase: WorkflowPhase) =>
  fetchJson<WorkflowContextPayload | null>(`/workflow/context/latest?phase=${phase}`);
export const getLatestWorkflowInsight = (phase: WorkflowPhase, producer = "the_one") =>
  fetchJson<WorkflowInsightResponse>(`/workflow/insight/latest?phase=${phase}&producer=${encodeURIComponent(producer)}`);
export const upsertWorkflowInsight = (data: WorkflowInsightUpsertRequest) =>
  fetch(`${API_BASE}/workflow/insight`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const runPreMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/pre/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());
export const runPostMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/post/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());

// Summary
export const getTradingStatus = () => fetchJson<any>("/summary/trading-status");
export const getWatchlist = () => fetchJson<any>("/summary/watchlist");
export const updateWatchlist = (data: any) => fetch(`${API_BASE}/summary/watchlist`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
