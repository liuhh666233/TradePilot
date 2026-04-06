import { Card, Col, List, Row, Tabs, Tag, Typography } from "antd";

const { Paragraph, Text } = Typography;

export function newsDirectionColor(direction?: string) {
  return (
    {
      positive: "green",
      mixed: "orange",
      negative: "red",
      neutral: "default",
    } as Record<string, string>
  )[direction || ""] || "default";
}

export function newsDirectionLabel(direction?: string) {
  return (
    {
      positive: "偏利多",
      mixed: "待观察",
      negative: "偏风险",
      neutral: "中性",
    } as Record<string, string>
  )[direction || ""] || "待观察";
}

export function newsCategoryLabel(category?: string) {
  return (
    {
      macro: "宏观政策",
      company: "个股公告",
      industry: "行业动态",
      geopolitics: "地缘政治",
      overseas: "海外市场",
      technology: "技术趋势",
      general: "综合资讯",
    } as Record<string, string>
  )[category || ""] || category || "未分类";
}

type NewsItem = {
  title?: string;
  source?: string;
  published_at?: string | null;
  url?: string | null;
  category?: string;
};

type SectorMapping = {
  sector_name?: string;
  role?: string;
  direction?: string;
  matched_count?: number;
  thesis?: string;
  related_news?: Array<{
    title?: string;
    url?: string | null;
    matched_aliases?: string[];
  }>;
};

type Props = {
  highlights: NewsItem[];
  categorizedNews: Array<[string, unknown]>;
  sectorMappings: SectorMapping[];
};

function renderNewsMeta(item: NewsItem) {
  return (
    <div style={{ fontSize: 12, color: "#666" }}>
      {item.category ? <Tag style={{ marginInlineEnd: 6 }}>{newsCategoryLabel(item.category)}</Tag> : null}
      {item.source || "unknown"}
      {item.published_at ? ` · ${String(item.published_at).slice(0, 16).replace("T", " ")}` : ""}
    </div>
  );
}

function renderDefaultNewsItem(item: NewsItem) {
  return (
    <div>
      <div style={{ fontWeight: 500 }}>
        {item.url ? (
          <a href={item.url} target="_blank" rel="noreferrer">{item.title || "未命名新闻"}</a>
        ) : (
          item.title || "未命名新闻"
        )}
      </div>
      {renderNewsMeta(item)}
    </div>
  );
}

function renderTechnologyNewsItem(item: NewsItem) {
  const title = item.title || "未命名新闻";
  const [repoName, ...rest] = title.split(": ");
  const description = rest.join(": ");
  const hasRepoLayout = Boolean(description) && repoName.includes("/");
  if (!hasRepoLayout) {
    return renderDefaultNewsItem(item);
  }
  return (
    <div>
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <Tag color="geekblue">repo</Tag>
        {item.url ? (
          <a href={item.url} target="_blank" rel="noreferrer" style={{ fontWeight: 600 }}>{repoName}</a>
        ) : (
          <Text strong>{repoName}</Text>
        )}
      </div>
      <Paragraph style={{ margin: "6px 0 6px", color: "#666" }} ellipsis={{ rows: 2, expandable: false }}>
        {description}
      </Paragraph>
      {renderNewsMeta(item)}
    </div>
  );
}

function renderNewsItem(item: NewsItem, variant?: "technology") {
  if (variant === "technology") {
    return renderTechnologyNewsItem(item);
  }
  return renderDefaultNewsItem(item);
}

export default function OvernightNewsTabs({ highlights, categorizedNews, sectorMappings }: Props) {
  const categoryTabs = categorizedNews.map(([category, items]) => {
    const data = Array.isArray(items) ? items.slice(0, 8) : [];
    return {
      key: category,
      label: (
        <span>
          {newsCategoryLabel(category)}
          <Tag style={{ marginInlineStart: 8 }}>{data.length}</Tag>
        </span>
      ),
      children: (
        <List
          size="small"
          dataSource={data}
          locale={{ emptyText: "暂无分类新闻" }}
          renderItem={(item) => <List.Item>{renderNewsItem(item as NewsItem, category === "technology" ? "technology" : undefined)}</List.Item>}
        />
      ),
    };
  });

  return (
    <Row gutter={[12, 12]}>
      <Col xs={24} lg={14}>
        <Card size="small" title="重点资讯">
          <List
            size="small"
            dataSource={highlights}
            locale={{ emptyText: "暂无夜间信息" }}
            renderItem={(item) => <List.Item>{renderNewsItem(item)}</List.Item>}
          />
        </Card>
      </Col>
      <Col xs={24} lg={10}>
        <Card size="small" title="相关观察板块">
          <List
            size="small"
            dataSource={sectorMappings}
            locale={{ emptyText: "暂无板块映射" }}
            renderItem={(item) => (
              <List.Item>
                <div style={{ width: "100%" }}>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <Text strong>{item.sector_name}</Text>
                    {item.role ? <Tag color="blue">{item.role}</Tag> : null}
                    {item.direction ? <Tag color={newsDirectionColor(item.direction)}>{newsDirectionLabel(item.direction)}</Tag> : null}
                    {item.matched_count ? <Tag>{item.matched_count} 条</Tag> : null}
                  </div>
                  {item.thesis ? <div style={{ marginTop: 4, fontSize: 12, color: "#666" }}>{item.thesis}</div> : null}
                  {(item.related_news || []).length > 0 ? (
                    <div style={{ marginTop: 6 }}>
                      {(item.related_news || []).map((news) => (
                        <div key={`${item.sector_name}-${news.title}`} style={{ fontSize: 12, marginBottom: 4 }}>
                          {news.url ? (
                            <a href={news.url} target="_blank" rel="noreferrer">{news.title}</a>
                          ) : (
                            <Text>{news.title}</Text>
                          )}
                          {(news.matched_aliases || []).length > 0 ? (
                            <span style={{ color: "#999" }}> · 匹配 {news.matched_aliases.join("/")}</span>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              </List.Item>
            )}
          />
        </Card>
      </Col>
      <Col xs={24}>
        <Card size="small" title="分类新闻模块">
          <Tabs
            items={categoryTabs.length > 0 ? categoryTabs : [{ key: "empty", label: "暂无分类", children: <Text type="secondary">暂无分类新闻</Text> }]}
          />
        </Card>
      </Col>
    </Row>
  );
}
