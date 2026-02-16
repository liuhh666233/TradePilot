import { useEffect, useState } from "react";
import { Card, Table, Tag, Tabs } from "antd";
import { getSectorRotation } from "../../services/api";

export default function SectorMap() {
  const [data, setData] = useState<any>(null);
  const [period, setPeriod] = useState("change_5d");

  useEffect(() => { getSectorRotation().then(setData); }, []);

  const sectors = data?.sectors || [];
  const sorted = [...sectors].sort((a: any, b: any) => (b[period] || 0) - (a[period] || 0));

  return (
    <div>
      <Card title="行业轮动" size="small" style={{ marginBottom: 16 }}>
        <Tabs
          activeKey={period}
          onChange={setPeriod}
          items={[
            { key: "change_5d", label: "5日涨幅" },
            { key: "change_20d", label: "20日涨幅" },
            { key: "change_60d", label: "60日涨幅" },
          ]}
        />
        <Table
          dataSource={sorted}
          rowKey="sector"
          size="small"
          pagination={false}
          columns={[
            { title: "板块", dataIndex: "sector", width: 100 },
            { title: "涨幅", dataIndex: period, width: 80, render: (v: number) => <span style={{ color: v >= 0 ? "#cf1322" : "#3f8600" }}>{v?.toFixed(2)}%</span> },
            { title: "PB", dataIndex: "avg_pb", width: 60, render: (v: number) => v?.toFixed(2) },
            { title: "PE", dataIndex: "avg_pe", width: 60, render: (v: number) => v?.toFixed(1) },
            {
              title: "标记", width: 120,
              render: (_: any, r: any) => {
                const tags = [];
                if (data?.high_positions?.some((h: any) => h.sector === r.sector)) tags.push(<Tag key="h" color="red">高位预警</Tag>);
                if (data?.low_opportunities?.some((l: any) => l.sector === r.sector)) tags.push(<Tag key="l" color="green">低位机会</Tag>);
                return tags;
              },
            },
          ]}
        />
      </Card>

      {data?.switch_suggestions?.length > 0 && (
        <Card title="高切低建议" size="small">
          {data.switch_suggestions.map((s: any, i: number) => (
            <Tag key={i} color="blue" style={{ margin: 4, padding: "4px 8px" }}>
              {s.from_sector} → {s.to_sector}
            </Tag>
          ))}
        </Card>
      )}
    </div>
  );
}
