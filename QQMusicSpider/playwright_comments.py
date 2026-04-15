# -*- coding: utf-8 -*-
"""
通过 Playwright 浏览器自动化获取 QQ 音乐评论的回退方案。
当 API 方式无法获取评论时使用。
"""

from pathlib import Path

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency
    PlaywrightError = Exception
    sync_playwright = None

from QQMusicSpider.utils import random_user_agent

PLAYWRIGHT_USER_AGENT = random_user_agent()
SONG_DETAIL_URL_TEMPLATE = "https://y.qq.com/n/ryqq/songDetail/{song_mid}"
COMMENT_ITEM_SELECTORS = [
    ".comment__list .comment__item",
    ".js_comment_list li",
    "[data-role='comment-item']",
]
COMMENT_TEXT_SELECTORS = [
    ".comment__text",
    ".comment__content",
    "[data-role='comment-text']",
]
COMMENT_USER_SELECTORS = [
    ".comment__name",
    ".js_comment_nick",
    "[data-role='comment-user']",
]


class QQMusicPlaywrightError(Exception):
    pass


def compact_whitespace(value):
    if value is None:
        return None
    collapsed = " ".join(str(value).split())
    return collapsed or None


def first_text(container, selectors):
    for selector in selectors:
        try:
            locator = container.locator(selector)
            count = min(locator.count(), 5)
            for index in range(count):
                value = compact_whitespace(locator.nth(index).inner_text(timeout=1200))
                if value:
                    return value
        except PlaywrightError:
            continue
    return None


def scrape_visible_comments(page, max_comments=10):
    try:
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(1000)
        page.mouse.wheel(0, 1800)
        page.wait_for_timeout(1000)
    except PlaywrightError:
        return []

    comment_items = []
    for selector in COMMENT_ITEM_SELECTORS:
        try:
            locator = page.locator(selector)
            count = min(locator.count(), max_comments)
            if count == 0:
                continue

            for index in range(count):
                item = locator.nth(index)
                text = first_text(item, COMMENT_TEXT_SELECTORS)
                if not text:
                    continue
                comment_items.append(
                    {
                        "comment_name": first_text(item, COMMENT_USER_SELECTORS),
                        "comment_text": text,
                    }
                )
            if comment_items:
                return comment_items
        except PlaywrightError:
            continue
    return comment_items


def apply_browser_stealth(context):
    context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        window.chrome = window.chrome || { runtime: {} };
        """
    )


def default_comment_profile_dir():
    current_project_profile = Path(__file__).resolve().parents[1] / ".playwright_profile"
    sibling_profile = Path(__file__).resolve().parents[2] / "qqmusic_public_metadata_scraper" / ".playwright_profile"
    if current_project_profile.exists():
        return current_project_profile
    if sibling_profile.exists():
        return sibling_profile
    return None


def normalize_top_comments(raw_comments):
    normalized = []
    for comment in raw_comments or []:
        if not isinstance(comment, dict):
            continue
        normalized.append(
            {
                "comment_name": compact_whitespace(comment.get("Nick")),
                "comment_text": compact_whitespace(comment.get("Content")),
            }
        )
    return [item for item in normalized if item.get("comment_text")]


# JS 注入脚本：通过 QQ 音乐页面内部 webpack 模块获取评论
_EVALUATE_SCRIPT = """
async ({ mid, songtype, topCommentsLimit }) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  let lastError = "";

  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      const chunkId = 900000 + Math.floor(Math.random() * 100000) + attempt;
      window.webpackJsonp.push([[chunkId], {
        [chunkId]: function(module, exports, req) {
          window.__qqReq = req;
        }
      }, [[chunkId]]]);

      const req = window.__qqReq;
      if (!req || !req.m || !req.m[447]) {
        lastError = "QQ Music bundle module 447 is unavailable";
        await sleep(500);
        continue;
      }

      const api = req(447);
      const detailResult = await api.k({ mid, songid: 0, songtype: Number(songtype) || 0 });
      let topComments = [];
      let topCommentsError = null;

      try {
        const requesterModule = req.m[7] ? req(7) : null;
        const requester = requesterModule && requesterModule.j ? requesterModule.j() : null;
        const song = detailResult && detailResult.songList && detailResult.songList[0];
        const bizIdCandidates = [];

        const addCandidate = (value) => {
          if (value === undefined || value === null || value === "") return;
          const normalized = typeof value === "string" ? value : String(value);
          if (!bizIdCandidates.includes(normalized)) bizIdCandidates.push(normalized);
        };

        addCandidate(song && song.id);
        addCandidate(song && song.mid);
        addCandidate(mid);

        if (requester && topCommentsLimit > 0) {
          for (const bizId of bizIdCandidates) {
            let candidateSucceeded = false;
            let pageNum = 0;
            let lastCommentSeqNo = "";
            let candidateComments = [];
            const seen = {};

            for (let guard = 0; guard < 12 && candidateComments.length < topCommentsLimit; guard += 1) {
              const pageSize = Math.min(25, topCommentsLimit - candidateComments.length);
              const response = await requester.request({
                module: "music.globalComment.CommentRead",
                method: "GetHotCommentList",
                param: {
                  BizType: 1, BizId: bizId,
                  LastCommentSeqNo: lastCommentSeqNo, PageSize: pageSize,
                  PageNum: pageNum, HotType: 1,
                  WithAirborne: 0, PicEnable: 1
                }
              });

              const first = Array.isArray(response) ? response[0] : response;
              if (!first) { topCommentsError = "Empty comment response"; break; }
              if (first.code !== 0) { topCommentsError = `Comment API code ${first.code}`; break; }

              candidateSucceeded = true;
              const data = first.data || {};
              const commentGroup = data.CommentList || data;
              const commentItems = Array.isArray(commentGroup.Comments)
                ? commentGroup.Comments
                : Array.isArray(data.Comments) ? data.Comments : [];

              for (const item of commentItems) {
                const commentId = item && item.CmId !== undefined && item.CmId !== null ? String(item.CmId) : "";
                if (!commentId || seen[commentId]) continue;
                seen[commentId] = true;
                candidateComments.push(item);
                if (candidateComments.length >= topCommentsLimit) break;
              }

              const nextSeq = commentItems.length && commentItems[commentItems.length - 1] && commentItems[commentItems.length - 1].SeqNo
                ? String(commentItems[commentItems.length - 1].SeqNo) : lastCommentSeqNo;
              const nextOffset = Number(commentGroup.NextOffset);
              const hasMore = Boolean(commentGroup.HasMore ?? data.HasMore);
              if (!hasMore || commentItems.length === 0) break;
              if (nextSeq && nextSeq === lastCommentSeqNo && !(Number.isFinite(nextOffset) && nextOffset > pageNum)) break;

              lastCommentSeqNo = nextSeq;
              pageNum = Number.isFinite(nextOffset) && nextOffset > pageNum ? nextOffset : pageNum + 1;
            }

            if (candidateSucceeded) {
              topComments = candidateComments;
              topCommentsError = null;
              break;
            }
          }
        }
      } catch (error) {
        topCommentsError = String(error);
      }

      return { ok: true, topComments, topCommentsError };
    } catch (error) {
      lastError = String(error);
      await sleep(800);
    }
  }

  return { ok: false, error: lastError || "QQ Music bundle API failed" };
}
"""


def fetch_hot_comments_via_playwright(
    song_mid,
    top_comments_limit=100,
    user_data_dir=None,
    headful=False,
    browser_channel="msedge",
    wait_seconds=8.0,
):
    if sync_playwright is None:
        raise QQMusicPlaywrightError("Playwright is not installed")

    song_url = SONG_DETAIL_URL_TEMPLATE.format(song_mid=song_mid)
    profile_dir = Path(user_data_dir) if user_data_dir else default_comment_profile_dir()

    try:
        with sync_playwright() as playwright:
            browser = None
            context = None
            common_context_args = {
                "locale": "zh-CN",
                "timezone_id": "Asia/Shanghai",
                "viewport": {"width": 1440, "height": 900},
                "user_agent": PLAYWRIGHT_USER_AGENT,
            }
            launch_kwargs = {
                "headless": not headful,
                "args": ["--disable-blink-features=AutomationControlled"],
            }
            if browser_channel:
                launch_kwargs["channel"] = browser_channel

            try:
                if profile_dir is not None:
                    context = playwright.chromium.launch_persistent_context(
                        user_data_dir=str(profile_dir),
                        **launch_kwargs,
                        **common_context_args,
                    )
                else:
                    browser = playwright.chromium.launch(**launch_kwargs)
                    context = browser.new_context(**common_context_args)

                apply_browser_stealth(context)
                page = context.pages[0] if context.pages else context.new_page()
                page.goto(song_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(int(max(wait_seconds, 0) * 1000))
                result = page.evaluate(
                    _EVALUATE_SCRIPT,
                    {"mid": song_mid, "songtype": 0, "topCommentsLimit": top_comments_limit},
                )
                if isinstance(result, dict) and result.get("ok"):
                    comments = normalize_top_comments(result.get("topComments"))
                    if comments:
                        return comments, "playwright_bundle"
                    visible_comments = scrape_visible_comments(page, max_comments=min(10, top_comments_limit))
                    if visible_comments:
                        return visible_comments, "playwright_dom"
                    top_comments_error = result.get("topCommentsError") or "Playwright fallback returned no comments"
                    raise QQMusicPlaywrightError(top_comments_error)
                if isinstance(result, dict):
                    raise QQMusicPlaywrightError(result.get("error") or "Playwright fallback failed")
                raise QQMusicPlaywrightError("Playwright fallback returned an unexpected response")
            finally:
                if context is not None:
                    context.close()
                if browser is not None:
                    browser.close()
    except Exception as exc:
        raise QQMusicPlaywrightError(str(exc)) from exc
