import { useEffect, useMemo, useState } from "react";
import { Alert, Button, Card, Col, Divider, List, Row, Space, Statistic, Table, Tabs, Tag, Typography } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import {
  getLatestWorkflow,
  getSchedulerHistory,
  getSchedulerStatus,
  getWorkflowHistory,
  getWorkflowStatus,
  runPostMarketWorkflow,
  runPreMarketWorkflow,
} from "../../services/api";

const { Paragraph, Text, Title } = Typography;

type WorkflowPhase = "pre_market" | "post_market";

function statusColor(status?: string) {
  return (
    {
      success: "green",
      partial: "orange",
      failed: "red",
      skipped: "default",
    } as Record<string, string>
  )[status || ""] || "default";
}

function stepStatusColor(status?: string) {
  return (
    {
      success: "green",
      failed: "red",
      skipped: "default",
      partial: "orange",
    } as Record<string, string>
  )[status || ""] || "default";
}

export default function Dashboard() {
  const [activePhase, setActivePhase] = useState<WorkflowPhase>("pre_market");
  const [preWorkflow, setPreWorkflow] = useState<any | null>(null);
  const [postWorkflow, setPostWorkflow] = useState<any | null>(null);
  const [workflowStatus, setWorkflowStatus] = useState<any>(null);
  const [workflowHistory, setWorkflowHistory] = useState<any[]>([]);
  const [schedulerStatus, setSchedulerStatus] = useState<any>(null);
  const [schedulerHistory, setSchedulerHistory] = useState<any[]>([]);
  const [runningPhase, setRunningPhase] = useState<WorkflowPhase | null>(null);
  const [loading, setLoading] = useState(false);

  const refreshData = async () => {
    setLoading(true);
    try {
      const [pre, post, status, history, scheduler, schedulerRuns] = await Promise.all([
        getLatestWorkflow("pre_market"),
        getLatestWorkflow("post_market"),
        getWorkflowStatus(),
        getWorkflowHistory(10),
        getSchedulerStatus(),
        getSchedulerHistory(10),
      ]);
      setPreWorkflow(pre);
      setPostWorkflow(post);
      setWorkflowStatus(status);
      setWorkflowHistory(history);
      setSchedulerStatus(scheduler);
      setSchedulerHistory(schedulerRuns);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshData();
  }, []);

  const handleRunWorkflow = async (phase: WorkflowPhase) => {
    setRunningPhase(phase);
    try {
      if (phase === "pre_market") {
        await runPreMarketWorkflow();
      } else {
        await runPostMarketWorkflow();
      }
      await refreshData();
    } finally {
      setRunningPhase(null);
    }
  };

  const currentWorkflow = useMemo(() => {
    return activePhase === "pre_market" ? preWorkflow?.run : postWorkflow?.run;
  }, [activePhase, postWorkflow, preWorkflow]);

  const summary = currentWorkflow?.summary;
  const steps = summary?.steps || [];
  const alerts = summary?.alerts || [];
  const watchContext = summary?.watch_context || {};
  const watchSectors = watchContext?.watch_sectors || summary?.watchlist?.watch_sectors || [];
  const watchStocks = watchContext?.watch_stocks || summary?.watchlist?.watch_stocks || [];
  const overnightNews = summary?.overnight_news || {};
  const yesterdayRecap = summary?.yesterday_recap || {};
  const todayWatchlist = summary?.today_watchlist || {};
  const actionFrame = summary?.action_frame || {};
  const marketOverview = summary?.market_overview || {};
  const sectorPositioning = summary?.sector_positioning || {};
  const positionHealth = summary?.position_health || {};
  const nextDayPrep = summary?.next_day_prep || {};
  const newsItems = overnightNews?.highlights || summary?.news?.items || [];
  const trackedItems = positionHealth?.tracked_items || [];
  const watchSectorRecords = sectorPositioning?.watch_sectors || [];

  return (
    <div>
      <Space direction="vertical" size={16} style={{ width: "100%" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div>
            <Title level={3} style={{ marginBottom: 4 }}>Daily Workflow</Title>
            <Text type="secondary">将首页收敛为盘前准备 / 盘后复盘两个阶段。</Text>
          </div>
          <Button icon={<ReloadOutlined />} onClick={refreshData} loading={loading}>刷新</Button>
        </div>

        <Row gutter={[12, 12]}>
          <Col xs={24} md={8}>
            <Card size="small" title="盘前状态">
              <Statistic
                title="最近结果"
                value={workflowStatus?.pre_market?.status || "暂无"}
                valueStyle={{ fontSize: 20 }}
                suffix={<Tag color={statusColor(workflowStatus?.pre_market?.status)}>{workflowStatus?.pre_market?.status || "none"}</Tag>}
              />
              <div style={{ marginTop: 8, color: "#666", fontSize: 12 }}>
                {workflowStatus?.pre_market?.finished_at ? String(workflowStatus.pre_market.finished_at).slice(0, 16).replace("T", " ") : "尚未执行"}
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" title="盘后状态">
              <Statistic
                title="最近结果"
                value={workflowStatus?.post_market?.status || "暂无"}
                valueStyle={{ fontSize: 20 }}
                suffix={<Tag color={statusColor(workflowStatus?.post_market?.status)}>{workflowStatus?.post_market?.status || "none"}</Tag>}
              />
              <div style={{ marginTop: 8, color: "#666", fontSize: 12 }}>
                {workflowStatus?.post_market?.finished_at ? String(workflowStatus.post_market.finished_at).slice(0, 16).replace("T", " ") : "尚未执行"}
              </div>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" title="调度器状态">
              <Tag color={schedulerStatus?.running ? "green" : "default"}>{schedulerStatus?.running ? "运行中" : "未运行"}</Tag>
              <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 6 }}>
                {(schedulerStatus?.jobs || []).map((job: any) => (
                  <div key={job.id} style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
                    <span>{job.id}</span>
                    <span style={{ color: "#666" }}>{job.next_run_time ? String(job.next_run_time).slice(5, 16).replace("T", " ") : "-"}</span>
                  </div>
                ))}
              </div>
            </Card>
          </Col>
        </Row>

        <Card size="small">
          <Tabs
            activeKey={activePhase}
            onChange={(key) => setActivePhase(key as WorkflowPhase)}
            items={[
              { key: "pre_market", label: "盘前准备" },
              { key: "post_market", label: "盘后复盘" },
            ]}
            tabBarExtraContent={
              <Space>
                <Button
                  size="small"
                  type={activePhase === "pre_market" ? "primary" : "default"}
                  loading={runningPhase === "pre_market"}
                  onClick={() => handleRunWorkflow("pre_market")}
                >
                  运行盘前
                </Button>
                <Button
                  size="small"
                  type={activePhase === "post_market" ? "primary" : "default"}
                  loading={runningPhase === "post_market"}
                  onClick={() => handleRunWorkflow("post_market")}
                >
                  运行盘后
                </Button>
              </Space>
            }
          />

          {currentWorkflow ? (
            <Space direction="vertical" size={16} style={{ width: "100%" }}>
              <Alert
                type={currentWorkflow.status === "failed" ? "error" : currentWorkflow.status === "partial" ? "warning" : "info"}
                showIcon
                message={`${summary?.title || "工作流"} · ${currentWorkflow.workflow_date}`}
                description={summary?.overview || currentWorkflow.error_message || "暂无摘要"}
              />

              <Row gutter={[12, 12]}>
                <Col xs={24} lg={12}>
                  <Card size="small" title="执行步骤">
                    <Table
                      dataSource={steps}
                      rowKey="name"
                      size="small"
                      pagination={false}
                      columns={[
                        { title: "步骤", dataIndex: "name", width: 150 },
                        {
                          title: "状态",
                          dataIndex: "status",
                          width: 100,
                          render: (value: string) => <Tag color={stepStatusColor(value)}>{value}</Tag>,
                        },
                        { title: "影响", dataIndex: "records_affected", width: 80 },
                        {
                          title: "错误",
                          dataIndex: "error_message",
                          render: (value: string | null) => value || "-",
                        },
                      ]}
                    />
                  </Card>
                </Col>
                <Col xs={24} lg={12}>
                  <Card size="small" title="关注池">
                    <Space direction="vertical" style={{ width: "100%" }} size={8}>
                      <div>
                        <Text strong>关注板块</Text>
                        <div style={{ marginTop: 6 }}>
                          {watchSectors.length > 0 ? watchSectors.map((sector: string) => <Tag key={sector}>{sector}</Tag>) : <Text type="secondary">暂无</Text>}
                        </div>
                      </div>
                      <Divider style={{ margin: "8px 0" }} />
                      <div>
                        <Text strong>关注股票</Text>
                        <div style={{ marginTop: 6 }}>
                          {watchStocks.length > 0 ? watchStocks.map((stock: any) => (
                            <Tag key={stock.code}>{stock.name ? `${stock.name}(${stock.code})` : stock.code}</Tag>
                          )) : <Text type="secondary">暂无</Text>}
                        </div>
                      </div>
                    </Space>
                  </Card>
                </Col>
              </Row>

              {activePhase === "pre_market" && (
                <Row gutter={[12, 12]}>
                  <Col xs={24} lg={12}>
                    <Card size="small" title="昨日复盘摘要">
                      <Space direction="vertical" style={{ width: "100%" }} size={8}>
                        <Paragraph style={{ marginBottom: 0 }}>{yesterdayRecap?.summary || "暂无上一交易日盘后结论。"}</Paragraph>
                        <Text type="secondary">市场状态：{yesterdayRecap?.regime || "unknown"}</Text>
                      </Space>
                    </Card>
                  </Col>
                  <Col xs={24} lg={12}>
                    <Card size="small" title="隔夜信息">
                      <List
                        size="small"
                        dataSource={newsItems}
                        locale={{ emptyText: "暂无夜间信息" }}
                        renderItem={(item: any) => (
                          <List.Item>
                            <div>
                              <div style={{ fontWeight: 500 }}>{item.title || "未命名新闻"}</div>
                              <div style={{ fontSize: 12, color: "#666" }}>
                                {item.source || "unknown"}
                                {item.published_at ? ` · ${String(item.published_at).slice(0, 16).replace("T", " ")}` : ""}
                              </div>
                            </div>
                          </List.Item>
                        )}
                      />
                    </Card>
                  </Col>
                  <Col xs={24} lg={14}>
                    <Card size="small" title="今日关注清单">
                      <Space direction="vertical" style={{ width: "100%" }} size={10}>
                        <div>
                          <Text strong>市场大势观察</Text>
                          <List
                            size="small"
                            dataSource={todayWatchlist?.market_checkpoints || []}
                            locale={{ emptyText: "暂无观察项" }}
                            renderItem={(item: string) => <List.Item>{item}</List.Item>}
                          />
                        </div>
                        <div>
                          <Text strong>重点板块</Text>
                          <div style={{ marginTop: 6 }}>
                            {(todayWatchlist?.focus_sectors || []).length > 0 ? (todayWatchlist.focus_sectors || []).map((item: any) => (
                              <Tag key={item.sector_name}>{item.sector_name}</Tag>
                            )) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                        <div>
                          <Text strong>持仓观察</Text>
                          <div style={{ marginTop: 6 }}>
                            {(todayWatchlist?.position_watch || []).length > 0 ? (todayWatchlist.position_watch || []).map((item: any) => (
                              <div key={item.code} style={{ fontSize: 12, marginBottom: 4 }}>
                                <Text strong>{item.name}</Text>
                                <Text type="secondary"> · {item.today_observation}</Text>
                              </div>
                            )) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                      </Space>
                    </Card>
                  </Col>
                  <Col xs={24} lg={10}>
                    <Card size="small" title="操作框架">
                      <Space direction="vertical" style={{ width: "100%" }} size={8}>
                        <Text>姿态：{actionFrame?.posture || "observe"}</Text>
                        <div>
                          <Text strong>关注方向</Text>
                          <div style={{ marginTop: 6 }}>
                            {(actionFrame?.focus_directions || []).length > 0 ? (actionFrame.focus_directions || []).map((item: string) => <Tag key={item}>{item}</Tag>) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                        <div>
                          <Text strong>风险提示</Text>
                          <List
                            size="small"
                            dataSource={actionFrame?.risk_warnings || []}
                            locale={{ emptyText: "暂无风险提示" }}
                            renderItem={(item: string) => <List.Item>{item}</List.Item>}
                          />
                        </div>
                      </Space>
                    </Card>
                  </Col>
                </Row>
              )}

              {activePhase === "post_market" && (
                <Row gutter={[12, 12]}>
                  <Col xs={24} lg={12}>
                    <Card size="small" title="市场大势">
                      <Space direction="vertical" style={{ width: "100%" }} size={8}>
                        <Paragraph style={{ marginBottom: 0 }}>{marketOverview?.summary || "暂无市场大势结论"}</Paragraph>
                        <Text type="secondary">市场状态：{marketOverview?.regime || "neutral"}</Text>
                        <div>
                          {(marketOverview?.key_takeaways || []).map((item: string) => (
                            <div key={item} style={{ fontSize: 12, color: "#666" }}>{item}</div>
                          ))}
                        </div>
                      </Space>
                    </Card>
                  </Col>
                  <Col xs={24} lg={12}>
                    <Card size="small" title="板块定位">
                      <Space direction="vertical" style={{ width: "100%" }} size={8}>
                        <div>
                          <Text strong>当日主线</Text>
                          <div style={{ marginTop: 6 }}>
                            {(sectorPositioning?.market_leaders || []).length > 0 ? (sectorPositioning.market_leaders || []).map((item: any) => (
                              <Tag key={item.sector_name}>{item.sector_name}</Tag>
                            )) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                        <div>
                          <Text strong>固定观察池</Text>
                          <div style={{ marginTop: 6 }}>
                            {watchSectorRecords.length > 0 ? watchSectorRecords.map((item: any) => (
                              <Tag key={item.sector_name}>{item.sector_name}</Tag>
                            )) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                      </Space>
                    </Card>
                  </Col>
                  <Col xs={24} lg={14}>
                    <Card size="small" title="持仓健康度">
                      <Table
                        dataSource={trackedItems}
                        rowKey={(item: any) => `${item.subject_type}-${item.code}`}
                        size="small"
                        pagination={false}
                        locale={{ emptyText: "暂无持仓/观察对象" }}
                        columns={[
                          { title: "对象", dataIndex: "name", width: 120 },
                          { title: "类型", dataIndex: "subject_type", width: 100 },
                          {
                            title: "状态",
                            dataIndex: "state",
                            width: 100,
                            render: (value: string) => <Tag color={stepStatusColor(value === "breakdown" ? "failed" : value === "breakout" ? "success" : value === "watch" ? "partial" : "skipped")}>{value}</Tag>,
                          },
                          {
                            title: "观察要点",
                            dataIndex: "observation_note",
                            render: (value: string) => value || "-",
                          },
                        ]}
                      />
                    </Card>
                  </Col>
                  <Col xs={24} lg={10}>
                    <Card size="small" title="明日准备">
                      <Space direction="vertical" style={{ width: "100%" }} size={8}>
                        <Text>市场偏向：{nextDayPrep?.market_bias || "observe"}</Text>
                        <div>
                          <Text strong>重点方向</Text>
                          <div style={{ marginTop: 6 }}>
                            {(nextDayPrep?.focus_sectors || []).length > 0 ? (nextDayPrep.focus_sectors || []).map((item: string) => <Tag key={item}>{item}</Tag>) : <Text type="secondary">暂无</Text>}
                          </div>
                        </div>
                        <div>
                          <Text strong>风险提示</Text>
                          <List
                            size="small"
                            dataSource={nextDayPrep?.risk_notes || []}
                            locale={{ emptyText: "暂无风险提示" }}
                            renderItem={(item: string) => <List.Item>{item}</List.Item>}
                          />
                        </div>
                      </Space>
                    </Card>
                  </Col>
                  <Col xs={24}>
                    <Card size="small" title="最近预警">
                      <List
                        size="small"
                        dataSource={alerts}
                        locale={{ emptyText: "暂无预警" }}
                        renderItem={(item: any) => (
                          <List.Item>
                            <Space direction="vertical" size={2} style={{ width: "100%" }}>
                              <div>
                                <Tag color={item.read_at ? "default" : "red"}>{item.urgency || "medium"}</Tag>
                                <Text strong>{item.title}</Text>
                              </div>
                              {item.message ? <Text type="secondary">{item.message}</Text> : null}
                            </Space>
                          </List.Item>
                        )}
                      />
                    </Card>
                  </Col>
                </Row>
              )}
            </Space>
          ) : (
            <Alert type="info" showIcon message="暂无工作流结果" description="先运行一次盘前或盘后 workflow。" />
          )}
        </Card>

        <Row gutter={[12, 12]}>
          <Col xs={24} lg={12}>
            <Card size="small" title="Workflow 历史">
              <Table
                dataSource={workflowHistory}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "阶段", dataIndex: "phase", width: 120 },
                  {
                    title: "状态",
                    dataIndex: "status",
                    width: 90,
                    render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                  },
                  { title: "触发", dataIndex: "triggered_by", width: 90 },
                  {
                    title: "时间",
                    dataIndex: "started_at",
                    render: (value: string) => String(value).slice(5, 16).replace("T", " "),
                  },
                ]}
              />
            </Card>
          </Col>
          <Col xs={24} lg={12}>
            <Card size="small" title="Scheduler 历史">
              <Table
                dataSource={schedulerHistory}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "任务", dataIndex: "job_name", width: 150 },
                  {
                    title: "状态",
                    dataIndex: "status",
                    width: 90,
                    render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                  },
                  { title: "影响", dataIndex: "records_affected", width: 70 },
                  {
                    title: "时间",
                    dataIndex: "started_at",
                    render: (value: string) => String(value).slice(5, 16).replace("T", " "),
                  },
                ]}
              />
            </Card>
          </Col>
        </Row>
      </Space>
    </div>
  );
}
