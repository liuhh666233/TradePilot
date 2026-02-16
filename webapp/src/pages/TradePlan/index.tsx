import { useEffect, useState } from "react";
import { Card, Button, Input, Modal, Form, InputNumber, Table, Tag, Space, Descriptions, List } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import { evaluateStock, getPlans, createPlan, updatePlanStatus, monitorPlan, deletePlan } from "../../services/api";

export default function TradePlan() {
  const [plans, setPlans] = useState<any[]>([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [evalResult, setEvalResult] = useState<any>(null);
  const [stockCode, setStockCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [monitorResult, setMonitorResult] = useState<any>(null);
  const [monitorId, setMonitorId] = useState<number | null>(null);
  const [form] = Form.useForm();

  const loadPlans = () => getPlans().then(setPlans);
  useEffect(() => { loadPlans(); }, []);

  const handleEvaluate = async () => {
    if (!stockCode) return;
    setLoading(true);
    const res = await evaluateStock(stockCode);
    setEvalResult(res);
    form.setFieldsValue({
      stock_code: stockCode,
      stock_name: stockCode,
      entry_target_price: res.support_price,
      stop_loss_pct: -10,
      take_profit_pct: 30,
    });
    setLoading(false);
  };

  const handleCreate = async (values: any) => {
    await createPlan(values);
    setModalOpen(false);
    setEvalResult(null);
    setStockCode("");
    form.resetFields();
    loadPlans();
  };

  const handleMonitor = async (id: number) => {
    const res = await monitorPlan(id);
    setMonitorResult(res);
    setMonitorId(id);
  };

  const handleActivate = async (id: number) => {
    await updatePlanStatus(id, { status: "active", entry_actual_price: 0, entry_triggered_at: new Date().toISOString().slice(0, 10) });
    loadPlans();
  };

  const handleDelete = async (id: number) => {
    await deletePlan(id);
    loadPlans();
  };

  const statusColor: Record<string, string> = { planning: "blue", active: "green", completed: "gold", cancelled: "default" };

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setModalOpen(true)}>新建交易计划</Button>
      </Space>

      <Table
        dataSource={plans}
        rowKey="id"
        size="small"
        columns={[
          { title: "股票", dataIndex: "stock_name", width: 100 },
          { title: "代码", dataIndex: "stock_code", width: 80 },
          { title: "状态", dataIndex: "status", width: 80, render: (v: string) => <Tag color={statusColor[v]}>{v}</Tag> },
          { title: "建仓价", dataIndex: "entry_target_price", width: 80, render: (v: number) => v?.toFixed(2) },
          { title: "止损", dataIndex: "stop_loss_price", width: 80, render: (v: number) => v?.toFixed(2) },
          { title: "止盈", dataIndex: "take_profit_price", width: 80, render: (v: number) => v?.toFixed(2) },
          { title: "评分", dataIndex: "composite_score", width: 60, render: (v: number) => v?.toFixed(0) },
          { title: "值博率", dataIndex: "risk_reward_ratio", width: 70, render: (v: number) => v?.toFixed(2) },
          {
            title: "操作", width: 200,
            render: (_: any, r: any) => (
              <Space>
                {r.status === "planning" && <Button size="small" type="primary" onClick={() => handleActivate(r.id)}>确认建仓</Button>}
                {r.status === "active" && <Button size="small" onClick={() => handleMonitor(r.id)}>监控</Button>}
                <Button size="small" danger onClick={() => handleDelete(r.id)}>删除</Button>
              </Space>
            ),
          },
        ]}
        expandable={{
          expandedRowRender: (r: any) => (
            <Descriptions size="small" column={2}>
              <Descriptions.Item label="建仓条件">{r.entry_conditions}</Descriptions.Item>
              <Descriptions.Item label="止损条件">{r.stop_loss_conditions}</Descriptions.Item>
              <Descriptions.Item label="止盈条件">{r.take_profit_conditions}</Descriptions.Item>
              <Descriptions.Item label="信号摘要">{r.signal_summary}</Descriptions.Item>
              <Descriptions.Item label="建仓理由">{r.entry_reason || "-"}</Descriptions.Item>
            </Descriptions>
          ),
        }}
      />

      {/* 监控结果 */}
      <Modal title={`监控计划 #${monitorId}`} open={!!monitorResult} onCancel={() => setMonitorResult(null)} footer={null}>
        {monitorResult && (
          <div>
            <p>当前价: {monitorResult.current_price?.toFixed(2)}</p>
            {monitorResult.stop_loss && (
              <Card size="small" title={<span>止损 {monitorResult.stop_loss.triggered ? <Tag color="red">触发</Tag> : <Tag color="green">安全</Tag>}</span>} style={{ marginBottom: 8 }}>
                <p>盈亏: {monitorResult.stop_loss.pnl_pct}%</p>
                <List size="small" dataSource={monitorResult.stop_loss.conditions} renderItem={(c: any) => <List.Item><Tag color={c.triggered ? "red" : "default"}>{c.name}</Tag></List.Item>} />
              </Card>
            )}
            {monitorResult.take_profit && (
              <Card size="small" title={<span>止盈 {monitorResult.take_profit.triggered ? <Tag color="gold">触发</Tag> : <Tag color="green">持有</Tag>}</span>}>
                <p>盈亏: {monitorResult.take_profit.pnl_pct}%</p>
                <List size="small" dataSource={monitorResult.take_profit.conditions} renderItem={(c: any) => <List.Item><Tag color={c.triggered ? "gold" : "default"}>{c.name}</Tag></List.Item>} />
              </Card>
            )}
          </div>
        )}
      </Modal>

      {/* 新建计划 */}
      <Modal title="新建交易计划" open={modalOpen} onCancel={() => { setModalOpen(false); setEvalResult(null); }} footer={null} width={640}>
        <Space style={{ marginBottom: 16 }}>
          <Input placeholder="股票代码" value={stockCode} onChange={(e) => setStockCode(e.target.value)} style={{ width: 150 }} />
          <Button onClick={handleEvaluate} loading={loading}>评估</Button>
        </Space>

        {evalResult && (
          <div style={{ marginBottom: 16 }}>
            <Card size="small" title={`评估结果: ${evalResult.score_label} (${evalResult.composite_score?.toFixed(0)}分)`}>
              <p>当前价: {evalResult.current_price} | 支撑位: {evalResult.support_price} | 值博率: {evalResult.risk_reward_ratio?.toFixed(2)}</p>
              <p>PE分位: {evalResult.pe_percentile}% | PB分位: {evalResult.pb_percentile}%</p>
              <div>
                <strong>建仓条件:</strong>
                {evalResult.entry_conditions?.map((c: string, i: number) => <Tag key={i} color="green" style={{ margin: 2 }}>{c}</Tag>)}
              </div>
              <div style={{ marginTop: 8 }}>
                <strong>信号:</strong>
                {evalResult.reasons?.map((r: string, i: number) => <div key={i}>{r}</div>)}
              </div>
            </Card>
          </div>
        )}

        {evalResult && (
          <Form form={form} onFinish={handleCreate} layout="vertical">
            <Form.Item name="stock_code" hidden><Input /></Form.Item>
            <Form.Item name="stock_name" label="股票名称"><Input /></Form.Item>
            <Form.Item name="entry_target_price" label="目标建仓价"><InputNumber style={{ width: "100%" }} /></Form.Item>
            <Form.Item name="entry_quantity" label="计划数量"><InputNumber style={{ width: "100%" }} /></Form.Item>
            <Form.Item name="entry_reason" label="建仓理由"><Input.TextArea rows={2} /></Form.Item>
            <Form.Item name="stop_loss_pct" label="止损比例(%)"><InputNumber style={{ width: "100%" }} /></Form.Item>
            <Form.Item name="take_profit_pct" label="止盈比例(%)"><InputNumber style={{ width: "100%" }} /></Form.Item>
            <Button type="primary" htmlType="submit">创建计划</Button>
          </Form>
        )}
      </Modal>
    </div>
  );
}
