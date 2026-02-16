import { useEffect, useState } from "react";
import { Card, Row, Col, Tabs, Tag, Table, Statistic, Space } from "antd";
import { ArrowUpOutlined, ArrowDownOutlined } from "@ant-design/icons";
import { Line } from "@ant-design/charts";
import { getIndexDaily, getMarketSentiment, getSectorRotation, getPositions } from "../../services/api";

const INDICES = [
  { code: "000001", name: "上证指数" },
  { code: "399001", name: "深证成指" },
  { code: "399006", name: "创业板指" },
  { code: "000688", name: "科创50" },
];

export default function Dashboard() {
  const [indexData, setIndexData] = useState<any[]>([]);
  const [activeIndex, setActiveIndex] = useState("000001");
  const [sentiment, setSentiment] = useState<any>(null);
  const [sectors, setSectors] = useState<any>(null);
  const [positions, setPositions] = useState<any[]>([]);

  useEffect(() => {
    getIndexDaily(activeIndex).then((d) => setIndexData(d.slice(-120)));
  }, [activeIndex]);

  useEffect(() => {
    getMarketSentiment().then(setSentiment);
    getSectorRotation().then(setSectors);
    getPositions().then(setPositions);
  }, []);

  const sentimentColor = (label: string) =>
    ({ "过热": "red", "偏热": "orange", "中性": "blue", "偏冷": "green" }[label] || "default");

  return (
    <div>
      <Row gutter={[16, 16]}>
        {/* 区块1: 大盘K线 */}
        <Col span={14}>
          <Card title="大盘指数" size="small">
            <Tabs
              activeKey={activeIndex}
              onChange={setActiveIndex}
              items={INDICES.map((i) => ({ key: i.code, label: i.name }))}
            />
            {indexData.length > 0 && (
              <Line
                data={indexData.map((d: any) => ({ date: String(d.date).slice(0, 10), value: d.close }))}
                xField="date"
                yField="value"
                height={260}
                xAxis={{ tickCount: 6 }}
                smooth
              />
            )}
          </Card>
        </Col>

        {/* 区块2: 资金面 */}
        <Col span={10}>
          <Card title="资金面" size="small">
            {sentiment ? (
              <Space direction="vertical" style={{ width: "100%" }}>
                <div>
                  市场情绪: <Tag color={sentimentColor(sentiment.sentiment?.label)}>
                    {sentiment.sentiment?.label} ({sentiment.sentiment?.score?.toFixed(0)})
                  </Tag>
                </div>
                <Statistic
                  title="北向资金(近5日)"
                  value={sentiment.northbound?.net_5d ? (sentiment.northbound.net_5d / 1e8).toFixed(1) : 0}
                  suffix="亿"
                  valueStyle={{ color: (sentiment.northbound?.net_5d || 0) >= 0 ? "#cf1322" : "#3f8600" }}
                  prefix={(sentiment.northbound?.net_5d || 0) >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                />
                <Statistic
                  title="融资余额日变化"
                  value={sentiment.margin?.daily_change ? (sentiment.margin.daily_change / 1e8).toFixed(1) : 0}
                  suffix="亿"
                  valueStyle={{ color: (sentiment.margin?.daily_change || 0) >= 0 ? "#cf1322" : "#3f8600" }}
                />
                <div>
                  <strong>ETF资金流(近5日):</strong>
                  {sentiment.etf && Object.entries(sentiment.etf).map(([code, v]: any) => (
                    <div key={code}>
                      {code}: <span style={{ color: v.net_5d >= 0 ? "#cf1322" : "#3f8600" }}>
                        {(v.net_5d / 1e8).toFixed(1)}亿
                      </span>
                    </div>
                  ))}
                </div>
              </Space>
            ) : "加载中..."}
          </Card>
        </Col>

        {/* 区块3: 行业热力图 */}
        <Col span={14}>
          <Card title="行业板块" size="small">
            {sectors?.sectors ? (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {sectors.sectors.slice(0, 10).map((s: any) => (
                  <Card
                    key={s.sector}
                    size="small"
                    style={{
                      width: "calc(20% - 8px)",
                      background: s.change_1d >= 0 ? "#fff1f0" : "#f6ffed",
                      borderColor: s.change_1d >= 0 ? "#ffa39e" : "#b7eb8f",
                    }}
                  >
                    <div style={{ fontWeight: "bold", fontSize: 12 }}>{s.sector}</div>
                    <div style={{ color: s.change_1d >= 0 ? "#cf1322" : "#3f8600", fontSize: 14 }}>
                      {s.change_1d >= 0 ? "+" : ""}{s.change_1d?.toFixed(2)}%
                    </div>
                    <div style={{ fontSize: 10, color: "#999" }}>60日: {s.change_60d?.toFixed(1)}%</div>
                  </Card>
                ))}
              </div>
            ) : "加载中..."}
            {sectors?.switch_suggestions?.length > 0 && (
              <div style={{ marginTop: 12 }}>
                <strong>高切低建议:</strong>
                {sectors.switch_suggestions.map((s: any, i: number) => (
                  <Tag key={i} color="blue" style={{ margin: 4 }}>{s.from_sector} → {s.to_sector}</Tag>
                ))}
              </div>
            )}
          </Card>
        </Col>

        {/* 区块4: 持仓盈亏 */}
        <Col span={10}>
          <Card title="持仓总览" size="small">
            {positions.length > 0 ? (
              <Table
                dataSource={positions}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "股票", dataIndex: "stock_name", width: 80 },
                  { title: "买入价", dataIndex: "buy_price", width: 70, render: (v: number) => v?.toFixed(2) },
                  { title: "数量", dataIndex: "quantity", width: 60 },
                  { title: "状态", dataIndex: "status", width: 60, render: (v: string) => <Tag color={v === "open" ? "green" : "default"}>{v}</Tag> },
                ]}
              />
            ) : <div style={{ color: "#999" }}>暂无持仓</div>}
          </Card>
        </Col>
      </Row>
    </div>
  );
}
