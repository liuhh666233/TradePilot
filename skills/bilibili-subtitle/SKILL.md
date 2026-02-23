---
name: bilibili-subtitle
description: 下载哔哩哔哩视频/合集的 AI 字幕并转换为 Markdown 文件
---

# 哔哩哔哩字幕下载

下载 B站视频或合集的 AI 生成字幕（ai-zh），转换为 Markdown 格式，按合集分目录存放。

## 前置条件

- yt-dlp：`/nix/store/2acpb7g39kx6h2bqb2dc8b690i3niacs-yt-dlp-2025.12.08/bin/yt-dlp`（如路径失效，用 `find /nix/store -name yt-dlp -type f` 查找，或 `nix-shell --pure -p yt-dlp-light`）
- cookies 文件：`bilibili_subs/cookies.txt`（Netscape 格式，必须包含 `SESSDATA`）

## 使用流程

### 1. 下载字幕

用户提供 B站 URL（视频或合集页面）。先下载到临时目录，再移到目标子目录：

```bash
# 合集下载（推荐先下到 /tmp 再移动，避免 -o 参数被干扰）
yt-dlp \
  --cookies bilibili_subs/cookies.txt \
  --write-subs --sub-lang ai-zh --sub-format srt --skip-download \
  -o '/tmp/bili_tmp/%(playlist_index)03d-%(title)s.%(ext)s' \
  '<URL>'

# 根据合集名创建子目录并移入
mkdir -p bilibili_subs/<合集名>
mv /tmp/bili_tmp/*.srt bilibili_subs/<合集名>/
```

单视频直接下载到目标目录：

```bash
yt-dlp \
  --cookies bilibili_subs/cookies.txt \
  --write-subs --sub-lang ai-zh --sub-format srt --skip-download \
  -o 'bilibili_subs/<目录>/%(title)s.%(ext)s' \
  '<URL>'
```

### 2. SRT 转 Markdown

```bash
python3 skills/bilibili-subtitle/srt2md.py bilibili_subs/<合集名>/
```

脚本会将目录下所有 `.srt` 文件转为同名 `.md`，去除序号、时间戳和重复行。

## 目录结构

```
bilibili_subs/
├── cookies.txt
├── <合集名1>/    # srt + md 文件
├── <合集名2>/
└── ...
```

## 注意事项

- cookies 中必须有 `SESSDATA` 字段，否则无法获取 AI 字幕
- 如 cookies 过期，需重新从浏览器导出（使用 "Get cookies.txt LOCALLY" 扩展）
- B站 AI 字幕语言代码为 `ai-zh`
- 合集 URL 格式：`https://space.bilibili.com/<mid>/lists/<sid>?type=season`
- 不要在 yt-dlp 命令中混用 `--print-to-file` 和 `-o`，会导致输出路径异常
