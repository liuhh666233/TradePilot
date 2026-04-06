import { Card, Col, Descriptions, List, Row, Space, Table, Tag, Typography } from "antd";

const { Paragraph, Text } = Typography;

type Props = {
  workflowDate?: string;
  requestedDate?: string | null;
  resolvedDate?: string | null;
  dateResolution?: string;
  marketOverview: Record<string, any>;
  sectorPositioning: Record<string, any>;
  positionHealth: Record<string, any>;
  nextDayPrep: Record<string, any>;
  crossDayReview: Record<string, any>;
  researchArchive: Record<string, any>;
};

function renderTagList(values?: any[], emptyText = "暂无") {
  if (!Array.isArray(values) || values.length === 0) {
    return <Text type="secondary">{emptyText}</Text>;
  }
  return values.map((value) => <Tag key={String(value)}>{String(value)}</Tag>);
}

function renderStringList(values?: any[], emptyText = "暂无") {
  if (!Array.isArray(values) || values.length === 0) {
    return <Text type="secondary">{emptyText}</Text>;
  }
  return <List size="small" dataSource={values} renderItem={(item) => <List.Item>{String(item)}</List.Item>} />;
}

export default function PostMarketPanels({
  workflowDate,
  requestedDate,
  resolvedDate,
  dateResolution,
  marketOverview,
  sectorPositioning,
  positionHealth,
  nextDayPrep,
  crossDayReview,
  researchArchive,
}: Props) {
  const indices = marketOverview?.indices || [];
  const breadth = marketOverview?.breadth || {};
  const limitStats = marketOverview?.limit_stats || {};
  const style = marketOverview?.style || {};
  const riskProxies = marketOverview?.risk_proxies || [];
  const marketLeaders = sectorPositioning?.market_leaders || [];
  const marketLaggards = sectorPositioning?.market_laggards || [];
  const watchSectors = sectorPositioning?.watch_sectors || [];
  const industryTop = sectorPositioning?.industry_top || [];
  const industryBottom = sectorPositioning?.industry_bottom || [];
  const conceptTop = sectorPositioning?.concept_top || [];
  const conceptBottom = sectorPositioning?.concept_bottom || [];
  const trackedItems = positionHealth?.tracked_items || [];
  const sectorHealth = positionHealth?.sector_health || [];
  const crossDayRows = crossDayReview?.sector_changes || crossDayReview?.watch_sector_changes || [];
  const archiveDownloads = researchArchive?.downloads || {};
  const topConsecutive = limitStats?.top_consecutive || [];

  return (
    <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <Card size="small" title="盘后摘要 / 日期说明" style={{ background: "#fafafa" }}>
        <Descriptions
          column={1}
          size="small"
          items={[
            { key: "workflow-date", label: "展示日期", children: workflowDate || "-" },
            { key: "requested-date", label: "请求日期", children: requestedDate || "-" },
            { key: "resolved-date", label: "解析日期", children: resolvedDate || "-" },
            { key: "resolution", label: "日期解析", children: dateResolution || "exact" },
          ]}
        />
        {requestedDate && resolvedDate && requestedDate !== resolvedDate ? (
          <Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
            当前为非交易日展示回退结果，默认展示最近一个有效交易日的盘后复盘。
          </Paragraph>
        ) : null}
      </Card>

      <Card size="small" title="市场大势" style={{ background: "#fafafa" }}>
        <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          <Paragraph style={{ marginBottom: 0 }}>{marketOverview?.summary || "暂无市场大势结论"}</Paragraph>
          <Descriptions
            column={2}
            size="small"
            items={[
              { key: "regime", label: "市场状态", children: marketOverview?.regime || "neutral" },
              { key: "confidence", label: "置信度", children: marketOverview?.confidence || "-" },
              { key: "up-down", label: "涨跌比", children: breadth?.up_down_ratio ?? "-" },
              { key: "ratio-5d", label: "5日中枢", children: breadth?.ratio_5d_avg ?? "-" },
              { key: "limit-up", label: "涨停/跌停/炸板", children: `${limitStats?.limit_up_count ?? "-"} / ${limitStats?.limit_down_count ?? "-"} / ${limitStats?.broken_board_count ?? "-"}` },
              { key: "style", label: "风格", children: style?.style_label || "-" },
            ]}
          />
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => String(item.index_name || item.index_code || Math.random())}
            dataSource={indices}
            locale={{ emptyText: "暂无指数表现" }}
            columns={[
              { title: "指数", dataIndex: "index_name", render: (value: string) => value || "-" },
              { title: "收盘", dataIndex: "close", render: (value: any) => value ?? "-" },
              { title: "涨跌幅", dataIndex: "pct_change", render: (value: any) => value ?? "-" },
              { title: "成交额", dataIndex: "amount", render: (value: any) => value ?? "-" },
            ]}
          />
          <Card size="small" type="inner" title="风险偏好代理">
            <Table
              size="small"
              pagination={false}
              rowKey={(item: any) => String(item.name || item.index_name || Math.random())}
              dataSource={riskProxies}
              locale={{ emptyText: "暂无风险代理" }}
              columns={[
                { title: "代理", dataIndex: "name", render: (value: string) => value || "-" },
                { title: "涨跌幅", dataIndex: "pct_change", render: (value: any) => value ?? "-" },
                { title: "说明", dataIndex: "description", render: (value: string) => value || "-" },
              ]}
            />
          </Card>
          {(marketOverview?.key_takeaways || []).length > 0 ? renderStringList(marketOverview.key_takeaways) : null}
        </Space>
      </Card>

      <Card size="small" title="板块定位" style={{ background: "#fafafa" }}>
        <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => String(item.sector_name || Math.random())}
            dataSource={marketLeaders}
            locale={{ emptyText: "暂无当日主线" }}
            title={() => "当日主线"}
            columns={[
              { title: "板块", dataIndex: "sector_name", render: (value: string) => value || "-" },
              { title: "涨跌幅", dataIndex: "pct_change", render: (value: any) => value ?? "-" },
              { title: "净流入", dataIndex: "net_flow", render: (value: any) => value ?? "-" },
              { title: "领涨股", dataIndex: "leader_stock", render: (value: string) => value || "-" },
            ]}
          />
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => String(`lag-${item.sector_name || Math.random()}`)}
            dataSource={marketLaggards}
            locale={{ emptyText: "暂无弱势板块" }}
            title={() => "弱势板块"}
            columns={[
              { title: "板块", dataIndex: "sector_name", render: (value: string) => value || "-" },
              { title: "涨跌幅", dataIndex: "pct_change", render: (value: any) => value ?? "-" },
              { title: "净流入", dataIndex: "net_flow", render: (value: any) => value ?? "-" },
              { title: "领涨股", dataIndex: "leader_stock", render: (value: string) => value || "-" },
            ]}
          />
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => String(`watch-${item.sector_name || Math.random()}`)}
            dataSource={watchSectors}
            locale={{ emptyText: "暂无固定观察池" }}
            title={() => "固定观察池"}
            columns={[
              { title: "板块", dataIndex: "sector_name", render: (value: string) => value || "-" },
              { title: "分类", dataIndex: "role", render: (value: string) => value || "-" },
              { title: "5日趋势", dataIndex: "trend_5d", render: (value: string) => value || "-" },
              { title: "一致性", dataIndex: "consistency", render: (value: any) => value ?? "-" },
              { title: "优选股", dataIndex: "leader_stock", render: (value: string) => value || "-" },
              { title: "观察重点", dataIndex: "observation_note", render: (value: string) => value || "-" },
            ]}
          />
          <Row gutter={[12, 12]}>
            <Col xs={24} lg={12}>
              <Card size="small" type="inner" title="行业板块 TOP / Bottom">
                <Table
                  size="small"
                  pagination={false}
                  rowKey={(item: any) => String(`industry-top-${item.name || Math.random()}`)}
                  dataSource={industryTop}
                  locale={{ emptyText: "暂无行业 TOP" }}
                  columns={[
                    { title: "行业", dataIndex: "name", render: (value: string) => value || "-" },
                    { title: "涨跌幅", dataIndex: "change_pct", render: (value: any) => value ?? "-" },
                    { title: "领涨股", dataIndex: "leader", render: (value: string) => value || "-" },
                  ]}
                />
                <div style={{ height: 12 }} />
                <Table
                  size="small"
                  pagination={false}
                  rowKey={(item: any) => String(`industry-bottom-${item.name || Math.random()}`)}
                  dataSource={industryBottom}
                  locale={{ emptyText: "暂无行业 Bottom" }}
                  columns={[
                    { title: "行业", dataIndex: "name", render: (value: string) => value || "-" },
                    { title: "涨跌幅", dataIndex: "change_pct", render: (value: any) => value ?? "-" },
                    { title: "领涨股", dataIndex: "leader", render: (value: string) => value || "-" },
                  ]}
                />
              </Card>
            </Col>
            <Col xs={24} lg={12}>
              <Card size="small" type="inner" title="概念板块 TOP / Bottom">
                <Table
                  size="small"
                  pagination={false}
                  rowKey={(item: any) => String(`concept-top-${item.name || Math.random()}`)}
                  dataSource={conceptTop}
                  locale={{ emptyText: "暂无概念 TOP" }}
                  columns={[
                    { title: "概念", dataIndex: "name", render: (value: string) => value || "-" },
                    { title: "涨跌幅", dataIndex: "change_pct", render: (value: any) => value ?? "-" },
                    { title: "领涨股", dataIndex: "leader", render: (value: string) => value || "-" },
                  ]}
                />
                <div style={{ height: 12 }} />
                <Table
                  size="small"
                  pagination={false}
                  rowKey={(item: any) => String(`concept-bottom-${item.name || Math.random()}`)}
                  dataSource={conceptBottom}
                  locale={{ emptyText: "暂无概念 Bottom" }}
                  columns={[
                    { title: "概念", dataIndex: "name", render: (value: string) => value || "-" },
                    { title: "涨跌幅", dataIndex: "change_pct", render: (value: any) => value ?? "-" },
                    { title: "领涨股", dataIndex: "leader", render: (value: string) => value || "-" },
                  ]}
                />
              </Card>
            </Col>
          </Row>
          <Card size="small" type="inner" title="观察重点">
            {renderStringList(sectorPositioning?.observation_focus || [], "暂无观察重点")}
          </Card>
        </Space>
      </Card>

      <Card size="small" title="涨停生态" style={{ background: "#fafafa" }}>
        <Descriptions
          column={2}
          size="small"
          items={[
            { key: "limit-up", label: "涨停池", children: limitStats?.limit_up_count ?? "-" },
            { key: "broken", label: "炸板", children: limitStats?.broken_board_count ?? "-" },
            { key: "limit-down", label: "跌停", children: limitStats?.limit_down_count ?? "-" },
            { key: "broken-rate", label: "炸板率", children: limitStats?.broken_board_rate ?? "-" },
            { key: "height", label: "连板高度", children: limitStats?.max_consecutive_board ?? "-" },
          ]}
        />
        <div style={{ marginTop: 12 }}>
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => String(item.code || item.name || Math.random())}
            dataSource={topConsecutive}
            locale={{ emptyText: "暂无连板梯队" }}
            columns={[
              { title: "代码", dataIndex: "code", render: (value: string) => value || "-" },
              { title: "名称", dataIndex: "name", render: (value: string) => value || "-" },
              { title: "连板数", dataIndex: "count", render: (value: any) => value ?? "-" },
              { title: "行业", dataIndex: "industry", render: (value: string) => value || "-" },
            ]}
          />
        </div>
      </Card>

      <Card size="small" title="跨日验证" style={{ background: "#fafafa" }}>
        {crossDayReview?.available === false ? (
          <Text type="secondary">{crossDayReview?.reason || "暂无跨日验证"}</Text>
        ) : (
          <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
            <Descriptions
              column={1}
              size="small"
              items={[
                { key: "period", label: "对照区间", children: `${crossDayReview?.previous_date || "-"} -> ${crossDayReview?.current_date || workflowDate || "-"}` },
                { key: "regime", label: "市场状态", children: `${crossDayReview?.previous_regime || "-"} -> ${crossDayReview?.current_regime || "-"}` },
                { key: "style", label: "风格状态", children: `${crossDayReview?.previous_style || "-"} -> ${crossDayReview?.current_style || "-"}` },
                { key: "breadth", label: "涨跌比", children: `${crossDayReview?.previous_breadth_ratio ?? "-"} -> ${crossDayReview?.current_breadth_ratio ?? "-"}` },
              ]}
            />
            <Table
              size="small"
              pagination={false}
              rowKey={(item: any) => String(item.sector_name || item.sector || Math.random())}
              dataSource={crossDayRows}
              locale={{ emptyText: "暂无观察板块变化" }}
              columns={[
                { title: "观察板块", dataIndex: "sector_name", render: (value: string, row: any) => value || row?.sector || "-" },
                { title: "昨日一致性", dataIndex: "previous_consistency", render: (value: any) => value ?? "-" },
                { title: "今日一致性", dataIndex: "current_consistency", render: (value: any) => value ?? "-" },
                { title: "变化", dataIndex: "change", render: (value: any) => value ?? "-" },
              ]}
            />
          </Space>
        )}
      </Card>

      <Card size="small" title="持仓健康度" style={{ background: "#fafafa" }}>
        <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          <Paragraph style={{ marginBottom: 0 }}>{positionHealth?.portfolio_health_summary || "暂无持仓健康度摘要"}</Paragraph>
          <Card size="small" type="inner" title="持仓板块健康度">
            <Table
              size="small"
              pagination={false}
              rowKey={(item: any) => String(item.sector_name || Math.random())}
              dataSource={sectorHealth}
              locale={{ emptyText: "暂无板块健康度" }}
              columns={[
                { title: "板块", dataIndex: "sector_name", render: (value: string) => value || "-" },
                { title: "状态", dataIndex: "state", render: (value: string) => value || "-" },
                { title: "观察要点", dataIndex: "observation_note", render: (value: string) => value || "-" },
              ]}
            />
          </Card>
          <Table
            size="small"
            pagination={false}
            rowKey={(item: any) => `${item.code || item.name}-${item.subject_type || item.role}`}
            dataSource={trackedItems}
            locale={{ emptyText: "暂无持仓/观察对象" }}
            columns={[
              { title: "对象", dataIndex: "name", render: (value: string) => value || "-" },
              { title: "代码", dataIndex: "code", render: (value: string) => value || "-" },
              { title: "角色", dataIndex: "role", render: (value: string) => value || "-" },
              { title: "主题", dataIndex: "theme", render: (value: string) => value || "-" },
              { title: "涨跌幅", dataIndex: "pct_change", render: (value: any) => value ?? "-" },
              { title: "换手率", dataIndex: "turnover_rate", render: (value: any) => value ?? "-" },
              { title: "量比", dataIndex: "volume_ratio", render: (value: any) => value ?? "-" },
              { title: "状态", dataIndex: "state", render: (value: string) => value || "-" },
              { title: "风险标记", dataIndex: "risk_flags", render: (value: any) => Array.isArray(value) && value.length > 0 ? value.join(" / ") : "暂无" },
              { title: "观察要点", dataIndex: "observation_note", render: (value: string) => value || "-" },
            ]}
          />
        </Space>
      </Card>

      <Card size="small" title="收盘研报归档" style={{ background: "#fafafa" }}>
        <Descriptions
          column={1}
          size="small"
          items={[
            { key: "window", label: "时间窗口", children: `${researchArchive?.begin_date || "-"} -> ${researchArchive?.end_date || "-"}` },
            { key: "output-root", label: "输出根目录", children: researchArchive?.output_root || "-" },
            { key: "categories", label: "归档类别", children: renderTagList(researchArchive?.categories, "暂无归档类别") },
            { key: "watch-sectors", label: "关注行业", children: renderTagList(researchArchive?.watch_sectors, "暂无关注行业") },
            { key: "watch-stocks", label: "关注个股", children: renderTagList(researchArchive?.watch_stocks, "暂无关注个股") },
            {
              key: "downloads",
              label: "下载数量",
              children: `macro ${(archiveDownloads?.macro || []).length} / industry ${(archiveDownloads?.industry || []).length} / stock ${(archiveDownloads?.stock || []).length}`,
            },
          ]}
        />
        <div style={{ marginTop: 12 }}>
          {renderStringList(researchArchive?.notes || [], "暂无归档说明")}
        </div>
      </Card>

      <Card size="small" title="明日准备" style={{ background: "#fafafa" }}>
        <Descriptions
          column={1}
          size="small"
          items={[
            { key: "bias", label: "市场偏向", children: nextDayPrep?.market_bias || "observe" },
            { key: "focus-sectors", label: "重点方向", children: renderTagList(nextDayPrep?.focus_sectors, "暂无重点方向") },
            { key: "focus-items", label: "重点对象", children: renderTagList(nextDayPrep?.focus_items, "暂无重点对象") },
            { key: "risk-notes", label: "风险提示", children: renderStringList(nextDayPrep?.risk_notes, "暂无风险提示") },
            { key: "checkpoints", label: "明日检查项", children: renderStringList(nextDayPrep?.tomorrow_checkpoints, "暂无检查项") },
          ]}
        />
      </Card>
    </Space>
  );
}
