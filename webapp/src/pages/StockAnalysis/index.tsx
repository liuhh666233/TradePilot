import { useEffect, useState } from "react";
import { Card, Select, Row, Col, Table, Tag } from "antd";
import { Line } from "@ant-design/charts";
import { getStocks, getTechnical, getValuation, getSignals } from "../../services/api";

export default function StockAnalysis() {
  const [stocks, setStocks] = useState<{code: string; name: string}[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [tech, setTech] = useState<any>(null);
  const [val, setVal] = useState<any>(null);
  const [signals, setSignals] = useState<any[]>([]);

  useEffect(() => { getStocks().then(setStocks); }, []);

  useEffect(() => {
    if (!selected) return;
    getTechnical(selected).then(setTech);
    getValuation(selected).then(setVal);
    getSignals(selected).then((r) => setSignals(r.signals || []));
  }, [selected]);

  const macdData = tech?.macd?.slice(-120) || [];

  return (
    <div>
      <Select
        showSearch
        placeholder="选择股票"
        style={{ width: 300, marginBottom: 16 }}
        onChange={setSelected}
        options={stocks.map((s) => ({ value: s.code, label: `${s.code} ${s.name}` }))}
        filterOption={(input, option) => (option?.label ?? "").toLowerCase().includes(input.toLowerCase())}
      />

      {selected && (
        <Row gutter={[16, 16]}>
          <Col span={16}>
            <Card title="K线 + MACD" size="small">
              {macdData.length > 0 && (
                <>
                  <Line
                    data={macdData.map((d: any) => ({ date: String(d.date).slice(0, 10), value: d.close }))}
                    xField="date" yField="value" height={200} xAxis={{ tickCount: 6 }} smooth
                  />
                  <Line
                    data={[
                      ...macdData.map((d: any) => ({ date: String(d.date).slice(0, 10), value: d.dif, type: "DIF" })),
                      ...macdData.map((d: any) => ({ date: String(d.date).slice(0, 10), value: d.dea, type: "DEA" })),
                    ]}
                    xField="date" yField="value" seriesField="type" height={120} xAxis={{ tickCount: 6 }}
                    color={["#cf1322", "#1890ff"]}
                  />
                </>
              )}
            </Card>
          </Col>

          <Col span={8}>
            <Card title="估值" size="small" style={{ marginBottom: 16 }}>
              {val ? (
                <div>
                  <p>PE分位: <Tag color={val.pe_percentile < 30 ? "green" : val.pe_percentile > 70 ? "red" : "blue"}>{val.pe_percentile}%</Tag></p>
                  <p>PB分位: <Tag color={val.pb_percentile < 30 ? "green" : val.pb_percentile > 70 ? "red" : "blue"}>{val.pb_percentile}%</Tag></p>
                  <p>值博率: <strong>{val.risk_reward_ratio?.toFixed(2)}</strong></p>
                </div>
              ) : "选择股票后加载"}
            </Card>

            <Card title="信号" size="small">
              <Table
                dataSource={signals}
                rowKey={(_, i) => String(i)}
                size="small"
                pagination={false}
                columns={[
                  { title: "信号", dataIndex: "name" },
                  { title: "类型", dataIndex: "type", render: (v: string) => {
                    const color = v.includes("golden") || v.includes("bull") || v.includes("low") || v.includes("breakout") ? "green" : "red";
                    return <Tag color={color}>{v}</Tag>;
                  }},
                  { title: "日期", dataIndex: "date", render: (v: string) => v ? String(v).slice(0, 10) : "-" },
                ]}
              />
            </Card>
          </Col>
        </Row>
      )}
    </div>
  );
}
