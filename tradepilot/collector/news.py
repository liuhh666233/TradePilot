"""Collect financial news from public market news feeds."""

from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from loguru import logger

from tradepilot.db import get_conn
from tradepilot.ingestion.models import NewsItemRecord

_NEWS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

NEWS_CATEGORIES: dict[str, list[str]] = {
    "macro": [
        "央行", "国务院", "政策", "降准", "降息", "MLF", "LPR", "逆回购", "财政", "货币", "监管", "证监", "银保监", "发改委", "国常会",
    ],
    "company": [
        "业绩", "减持", "增持", "回购", "分红", "并购", "重组", "定增", "解禁", "股权", "董事", "高管", "年报", "季报", "预增", "预减", "中报",
    ],
    "industry": [
        "板块", "产业", "产能", "订单", "出货", "扩产", "供需", "价格上涨", "涨价", "景气", "渗透率", "产业链",
    ],
    "geopolitics": [
        "冲突", "制裁", "关税", "战争", "谈判", "协议", "外交", "军事", "领土", "贸易摩擦", "中美",
    ],
    "overseas": [
        "美股", "美联储", "纳斯达克", "标普", "道琼斯", "Fed", "欧央行", "日央行", "外资", "北向", "港股", "恒生",
    ],
    "technology": [
        "GitHub", "开源", "AI", "LLM", "GPT", "Agent", "大模型", "机器人", "自动驾驶",
    ],
}


class NewsCollector:
    """Collect financial news from public feeds and persist to DuckDB."""

    def collect(self, stock_codes: list[str] | None = None) -> list[NewsItemRecord]:
        """Fetch and store news items.

        Args:
            stock_codes: Optional stock filter. If empty, fetch market-wide news.

        Returns:
            List of persisted news records.
        """
        logger.info("NewsCollector.collect called, stock_codes={}", stock_codes)
        items = (
            self._fetch_cls_telegraph(limit=40)
            + self._fetch_eastmoney_kuaixun(limit=30)
            + self._fetch_wallstreetcn(limit=20)
            + self._fetch_36kr(limit=15, keyword="融资,财报,上市,IPO,基金,投资,营收")
            + self._fetch_hn_algolia(limit=12, keyword="AI,LLM,GPT,Claude,Anthropic,DeepSeek,Agent", min_points=10, days=3)
            + self._fetch_github_trending(limit=10)
        )
        items = self._deduplicate(items)
        if stock_codes:
            items = self._filter_by_stock_codes(items, stock_codes)
        records = [self._to_record(item) for item in items]
        self._persist(records)
        return records

    def _fetch_cls_telegraph(self, limit: int = 30) -> list[dict]:
        """Fetch telegraph news from CLS."""
        api = "https://www.cls.cn/nodeapi/updateTelegraphList"
        params = {"app": "CailianpressWeb", "os": "web", "sv": "7.7.5", "rn": str(limit)}
        try:
            data = requests.get(api, params=params, headers=_NEWS_HEADERS, timeout=10).json()
        except Exception as exc:
            logger.warning("cls telegraph fetch failed: {}", exc)
            return []

        items: list[dict] = []
        for roll in data.get("data", {}).get("roll_data", []):
            title = roll.get("title") or roll.get("brief") or (roll.get("content") or "")[:80]
            if not title:
                continue
            content = roll.get("content") or roll.get("brief") or title
            share_url = roll.get("shareurl", "")
            item_id = str(roll.get("id") or self._hash_id(title, content))
            published_at = self._from_timestamp(roll.get("ctime"))
            subjects = [
                subject.get("subject_name", "")
                for subject in (roll.get("subjects") or [])
                if subject.get("subject_name")
            ]
            stocks = [
                stock.get("stock_code", "") or stock.get("secu_code", "")
                for stock in (roll.get("stock_list") or [])
                if stock.get("stock_code") or stock.get("secu_code")
            ]
            items.append(
                {
                    "source": "cls_telegraph",
                    "source_item_id": item_id,
                    "title": title,
                    "content": content,
                    "category": self._categorize(title, content, subjects, source="cls_telegraph"),
                    "published_at": published_at,
                    "url": share_url or f"https://www.cls.cn/detail/{item_id}",
                    "subjects": subjects,
                    "stock_codes": stocks,
                }
            )
        return items

    def _fetch_eastmoney_kuaixun(self, limit: int = 30) -> list[dict]:
        """Fetch kuaixun news from Eastmoney."""
        api = "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns"
        params = {
            "column": "350",
            "pageSize": str(limit),
            "pageIndex": "1",
            "client": "web",
            "biz": "web_news_col",
            "req_trace": str(int(datetime.now().timestamp() * 1000)),
        }
        try:
            data = requests.get(api, params=params, headers=_NEWS_HEADERS, timeout=10).json()
        except Exception as exc:
            logger.warning("eastmoney kuaixun fetch failed: {}", exc)
            return []

        items: list[dict] = []
        for article in (data.get("data") or {}).get("list", []):
            title = article.get("title", "")
            if not title:
                continue
            content = article.get("digest", "") or title
            item_id = str(article.get("code") or article.get("infoCode") or self._hash_id(title, content))
            published_at = self._parse_datetime(article.get("showTime"))
            items.append(
                {
                    "source": "eastmoney_kuaixun",
                    "source_item_id": item_id,
                    "title": title,
                    "content": content,
                    "category": self._categorize(title, content, source="eastmoney_kuaixun"),
                    "published_at": published_at,
                    "url": article.get("uniqueUrl", "") or article.get("url", ""),
                    "subjects": [],
                    "stock_codes": [],
                }
            )
        return items

    def _fetch_wallstreetcn(self, limit: int = 20) -> list[dict]:
        """Fetch articles from WallStreetCN global channel API."""
        api = (
            "https://api-one.wallstcn.com/apiv1/content/information-flow"
            "?channel=global-channel&accept=article&limit=30"
        )
        try:
            data = requests.get(api, headers=_NEWS_HEADERS, timeout=10).json()
        except Exception as exc:
            logger.warning("wallstreetcn fetch failed: {}", exc)
            return []
        items: list[dict] = []
        for entry in (data.get("data") or {}).get("items", [])[:limit]:
            resource = entry.get("resource", {})
            title = resource.get("title") or resource.get("content_short", "")
            if not title:
                continue
            url = resource.get("uri", "")
            timestamp = resource.get("display_time", 0)
            published_at = self._from_timestamp(timestamp) if timestamp else None
            items.append(
                {
                    "source": "wallstreetcn",
                    "source_item_id": str(resource.get("id") or self._hash_id(title, url or title)),
                    "title": title,
                    "content": resource.get("content_short") or title,
                    "category": self._categorize(title, resource.get("content_short") or title, source="wallstreetcn"),
                    "published_at": published_at,
                    "url": url,
                    "subjects": [],
                    "stock_codes": [],
                }
            )
        return items

    def _fetch_36kr(self, limit: int = 15, keyword: str | None = None) -> list[dict]:
        """Fetch newsflashes from 36Kr."""
        try:
            response = requests.get("https://36kr.com/newsflashes", headers=_NEWS_HEADERS, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as exc:
            logger.warning("36kr fetch failed: {}", exc)
            return []
        items: list[dict] = []
        for element in soup.select(".newsflash-item"):
            title_element = element.select_one(".item-title")
            if not title_element:
                continue
            title = title_element.get_text(strip=True)
            text = title.lower()
            if keyword:
                keywords = [item.strip().lower() for item in keyword.split(",") if item.strip()]
                if keywords and not any(item in text for item in keywords):
                    continue
            href = title_element.get("href", "")
            url = f"https://36kr.com{href}" if href and not href.startswith("http") else href
            items.append(
                {
                    "source": "36kr",
                    "source_item_id": self._hash_id(title, url or title),
                    "title": title,
                    "content": title,
                    "category": self._categorize(title, title, source="36kr"),
                    "published_at": None,
                    "url": url,
                    "subjects": [],
                    "stock_codes": [],
                }
            )
            if len(items) >= limit:
                break
        return items

    def _fetch_hn_algolia(
        self,
        limit: int = 20,
        keyword: str | None = None,
        min_points: int = 1,
        days: int = 1,
    ) -> list[dict]:
        """Fetch technology stories from Hacker News via Algolia."""
        since = int(time.time() - days * 86400)
        numeric = f"created_at_i>{since},points>{min_points}"
        query = ""
        if keyword:
            parts = [item.strip() for item in keyword.split(",") if item.strip()]
            quoted = [f'"{item}"' if " " in item else item for item in parts]
            query = " OR ".join(quoted)
        endpoint = "search_by_date" if query else "search"
        params: dict[str, str] = {
            "tags": "story",
            "numericFilters": numeric,
            "hitsPerPage": str(limit * 2),
        }
        if query:
            params["query"] = query
        try:
            data = requests.get(
                f"https://hn.algolia.com/api/v1/{endpoint}",
                params=params,
                headers=_NEWS_HEADERS,
                timeout=10,
            ).json()
        except Exception as exc:
            logger.warning("hn algolia fetch failed: {}", exc)
            return []
        items: list[dict] = []
        for hit in (data.get("hits") or [])[:limit]:
            title = hit.get("title") or ""
            if not title:
                continue
            url = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}"
            published_at = self._parse_iso_datetime(hit.get("created_at"))
            items.append(
                {
                    "source": "hacker_news",
                    "source_item_id": str(hit.get("objectID") or self._hash_id(title, url)),
                    "title": title,
                    "content": hit.get("story_text") or title,
                    "category": self._categorize(title, hit.get("story_text") or title, source="hacker_news"),
                    "published_at": published_at,
                    "url": url,
                    "subjects": [],
                    "stock_codes": [],
                }
            )
        return items

    def _fetch_github_trending(self, limit: int = 10) -> list[dict]:
        """Fetch trending GitHub repositories as technology trend items."""
        try:
            response = requests.get(
                "https://github.com/trending",
                params={"since": "daily"},
                headers=_NEWS_HEADERS,
                timeout=10,
            )
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception as exc:
            logger.warning("github trending fetch failed: {}", exc)
            return []
        items: list[dict] = []
        for row in soup.select("article.Box-row")[:limit]:
            name_element = row.select_one("h2 a")
            if not name_element:
                continue
            repo_path = name_element.get("href", "").strip("/")
            if not repo_path:
                continue
            desc_element = row.select_one("p")
            desc = desc_element.get_text(strip=True) if desc_element else ""
            stars_element = row.select_one("span.d-inline-block.float-sm-right")
            stars_text = stars_element.get_text(strip=True) if stars_element else "0"
            title = f"{repo_path}: {desc}" if desc else repo_path
            items.append(
                {
                    "source": "github_trending",
                    "source_item_id": repo_path,
                    "title": title,
                    "content": desc or title,
                    "category": "technology",
                    "published_at": None,
                    "url": f"https://github.com/{repo_path}",
                    "subjects": [],
                    "stock_codes": [],
                }
            )
        return items

    def _deduplicate(self, items: list[dict]) -> list[dict]:
        """Remove duplicate news items by source item id, URL, and normalized title."""
        seen_ids: set[str] = set()
        seen_urls: set[str] = set()
        seen_titles: set[str] = set()
        result: list[dict] = []
        for item in items:
            source_item_id = str(item.get("source_item_id") or "")
            title = str(item.get("title") or "")
            url = str(item.get("url") or "")
            title_key = re.sub(r"[\s\u3000:：,，。.!！?？【】\[\]()（）]", "", title)[:20]
            if source_item_id and source_item_id in seen_ids:
                continue
            if url and url in seen_urls:
                continue
            if title_key and title_key in seen_titles:
                continue
            if source_item_id:
                seen_ids.add(source_item_id)
            if url:
                seen_urls.add(url)
            if title_key:
                seen_titles.add(title_key)
            result.append(item)
        return result

    def _filter_by_stock_codes(self, items: list[dict], stock_codes: list[str]) -> list[dict]:
        """Filter items to stock-linked news when stock codes are provided."""
        stock_code_set = {code.strip() for code in stock_codes if code.strip()}
        if not stock_code_set:
            return items
        result: list[dict] = []
        for item in items:
            linked_codes = {str(code).strip() for code in item.get("stock_codes", []) if str(code).strip()}
            if linked_codes & stock_code_set:
                result.append(item)
        return result

    def _to_record(self, item: dict) -> NewsItemRecord:
        """Convert a fetched dict item to the persisted schema."""
        return NewsItemRecord(
            source=item["source"],
            source_item_id=str(item["source_item_id"]),
            title=item["title"],
            content=item.get("content", "") or item["title"],
            category=item.get("category"),
            published_at=item.get("published_at"),
            url=item.get("url"),
        )

    def _categorize(self, title: str, content: str, subjects: list[str] | None = None, source: str | None = None) -> str:
        """Categorize one news item using The-One style keyword groups with source-aware bias."""
        text = " ".join([title, content, " ".join(subjects or [])])
        text_lower = text.lower()
        source_name = (source or "").lower()

        technology_priority = ["github", "开源", "ai", "llm", "gpt", "agent", "大模型", "机器人", "自动驾驶", "英伟达", "算力", "芯片", "半导体"]
        company_priority = ["业绩", "财报", "融资", "ipo", "上市", "回购", "增持", "减持", "并购", "重组"]
        macro_priority = ["央行", "政策", "财政", "货币", "美联储", "fed", "利率", "降息", "加息"]
        geopolitics_priority = ["中美", "伊朗", "以色列", "俄", "乌", "战争", "冲突", "制裁", "外交", "关税"]

        if source_name in {"github_trending", "hacker_news"}:
            return "technology"
        if source_name in {"wallstreetcn", "36kr"} and any(keyword.lower() in text_lower for keyword in technology_priority):
            return "technology"
        if source_name in {"wallstreetcn", "36kr"} and any(keyword.lower() in text_lower for keyword in company_priority):
            return "company"
        if source_name in {"wallstreetcn", "36kr"} and any(keyword.lower() in text_lower for keyword in macro_priority):
            return "macro"
        if source_name in {"wallstreetcn", "36kr"} and any(keyword.lower() in text_lower for keyword in geopolitics_priority):
            return "geopolitics"

        for category, keywords in NEWS_CATEGORIES.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return category
        return "general"

    def _hash_id(self, title: str, content: str) -> str:
        """Build a deterministic fallback identifier for one news item."""
        payload = f"{title}|{content}".encode("utf-8", errors="ignore")
        return hashlib.md5(payload).hexdigest()

    def _from_timestamp(self, timestamp: int | float | str | None) -> datetime | None:
        """Convert a unix timestamp to datetime if possible."""
        if timestamp in (None, ""):
            return None
        try:
            return datetime.fromtimestamp(int(timestamp))
        except Exception:
            return None

    def _parse_datetime(self, value: str | None) -> datetime | None:
        """Parse Eastmoney datetime string."""
        if not value:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _parse_iso_datetime(self, value: str | None) -> datetime | None:
        """Parse ISO-like datetime strings from external APIs."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None

    def _persist(self, items: list[NewsItemRecord]) -> int:
        """Write news items to DuckDB, skipping duplicates.

        Args:
            items: News records to persist.

        Returns:
            Number of newly inserted rows.
        """
        if not items:
            return 0
        conn = get_conn()
        inserted = 0
        for item in items:
            try:
                before = conn.execute(
                    "SELECT url, category, published_at FROM news_items WHERE source = ? AND source_item_id = ? LIMIT 1",
                    [item.source, item.source_item_id],
                ).fetchone()
                item_url = item.url or None
                conn.execute(
                    """
                    INSERT INTO news_items (source, source_item_id, title, content, category, published_at, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [item.source, item.source_item_id, item.title, item.content, item.category, item.published_at, item_url],
                )
                if before is None:
                    inserted += 1
                else:
                    existing_url, existing_category, existing_published_at = before
                    if (not existing_url and item_url) or (not existing_category and item.category) or (existing_published_at is None and item.published_at is not None):
                        conn.execute(
                            """
                            UPDATE news_items
                            SET url = COALESCE(url, ?),
                                category = COALESCE(category, ?),
                                published_at = COALESCE(published_at, ?)
                            WHERE source = ? AND source_item_id = ?
                            """,
                            [item_url, item.category, item.published_at, item.source, item.source_item_id],
                        )
            except Exception:
                logger.exception("Failed to persist news item {}", item.source_item_id)
        return inserted
