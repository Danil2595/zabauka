import os, json, time, hashlib
from datetime import datetime, timedelta
import requests, feedparser, yaml
from bs4 import BeautifulSoup
from dateutil import tz, parser as dtparser

# === Общие настройки ===
TZ = tz.gettz("Europe/Minsk")
TOKEN = os.environ["TG_BOT_TOKEN"]
CHANNEL = os.environ.get("TG_CHANNEL")  # вида @имя_канала
MAX_POSTS = int(os.getenv("MAX_POSTS", "5"))

# === Память (anti-duplicate) ===
STATE_PATH = "data/state.json"

def load_state():
    os.makedirs("data", exist_ok=True)
    if os.path.exists(STATE_PATH):
        return json.load(open(STATE_PATH, "r", encoding="utf-8"))
    return {"posted_ids": []}

def save_state(s):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False, indent=2)

# === Утилиты ===
def now_iso():
    return datetime.now(TZ).isoformat()

def to_local(dt):
    if not dt.tzinfo:
        return dt.replace(tzinfo=tz.UTC).astimezone(TZ)
    return dt.astimezone(TZ)

def try_parse_date(txt):
    if not txt:
        return None
    try:
        return to_local(dtparser.parse(txt, fuzzy=True))
    except:
        return None

def make_id(source_name, title, link):
    raw = f"{source_name}|{title}|{link}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def load_sources():
    with open("config/sources.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("sources", [])

# === Картинки (og:image) ===
def fetch_og_image(url: str):
    try:
        html = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"}).text
        s = BeautifulSoup(html, "lxml")
        tag = s.find("meta", property="og:image")
        if tag and tag.get("content"):
            return tag["content"]
    except Exception as e:
        print("og:image error:", e)
    return None

# === Сбор RSS ===
def collect_rss(src):
    items = []
    feed = feedparser.parse(src["url"])
    for e in feed.entries:
        title = (e.get("title") or "").strip()
        link = e.get("link") or ""

        start = None
        if e.get("published_parsed"):
            start = to_local(datetime(*e.published_parsed[:6]))
        elif e.get("published") or e.get("updated"):
            start = try_parse_date(e.get("published") or e.get("updated"))

        # картинка из RSS
        img = None
        mc = e.get("media_content") or []
        if isinstance(mc, list) and mc:
            img = mc[0].get("url")
        if not img:
            mt = e.get("media_thumbnail") or []
            if isinstance(mt, list) and mt:
                img = mt[0].get("url")
        if not img and e.get("links"):
            for L in e["links"]:
                if L.get("type", "").startswith("image/") and L.get("href"):
                    img = L["href"]
                    break

        it = {
            "id": make_id(src["name"], title, link),
            "source": src["name"],
            "title": title or "Событие",
            "url": link,
            "start": start.isoformat() if start else None,
            "end": None,
            "place": None,
            "price": None,
            "category": src.get("category_hint"),
            "city": src.get("city") or "Minsk",
            "image": img,
            "collected_at": now_iso(),
        }
        items.append(it)
    return items

# === Сбор HTML ===
def collect_html(src):
    r = requests.get(src["url"], timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    cards = soup.select(src["list_selector"])
    out = []

    for card in cards:
        def tex(q):
            el = card.select_one(q) if q else None
            return el.get_text(strip=True) if el else None

        def href(q):
            el = card.select_one(q) if q else None
            return (el["href"] if el and el.has_attr("href") else None)

        title = tex(src.get("title_selector")) or "Событие"
        link = href(src.get("link_selector")) or src["url"]

        # картинка из карточки
        img = None
        img_sel = src.get("image_selector")
if img_sel:
            pic = card.select_one(img_sel)
            if pic and pic.has_attr("src"):
                img = pic["src"]
            elif pic and pic.has_attr("data-src"):
                img = pic["data-src"]

        # если нет — пробуем og:image на странице события
        if not img and link:
            img = fetch_og_image(link)

        date_text = tex(src.get("date_selector"))
        start = try_parse_date(date_text)

        it = {
            "id": make_id(src["name"], title, link),
            "source": src["name"],
            "title": title,
            "url": link,
            "start": start.isoformat() if start else None,
            "end": None,
            "place": tex(src.get("place_selector")),
            "price": tex(src.get("price_selector")),
            "category": src.get("category_hint"),
            "city": "Minsk",
            "image": img,
            "collected_at": now_iso(),
        }
        out.append(it)

    return out

# === Логика постинга ===
def is_future(start_iso):
    if not start_iso:
        return True
    dt = dtparser.isoparse(start_iso).astimezone(TZ)
    return dt >= datetime.now(TZ) - timedelta(hours=2)

def dedupe(items):
    seen, out = set(), []
    for it in items:
        sig = hashlib.md5((it.get("title", "") + (it.get("start") or "") + (it.get("place") or "")).encode("utf-8")).hexdigest()
        if sig in seen:
            continue
        seen.add(sig)
        out.append(it)
    return out

def format_post(it):
    date_line = ""
    if it.get("start"):
        d = dtparser.isoparse(it["start"]).astimezone(TZ)
        date_line = d.strftime("%d %B (%a) %H:%M")
    price = it.get("price") or "—"
    place = it.get("place") or "Минск"
    cat = it.get("category")
    tags = f"\n#{cat} #минск" if cat else "\n#минск"
    return (
        f"🎯 <b>{it.get('title','Событие')}</b>\n"
        f"📅 {date_line}\n"
        f"📍 {place}\n"
        f"💰 {price}\n"
        f"🔗 <a href=\"{it.get('url','')}\">Подробнее</a>"
        f"{tags}"
    )

def post_message(text, image=None):
    if image:
        url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        data = {
            "chat_id": CHANNEL,
            "photo": image,
            "caption": text,
            "parse_mode": "HTML",
        }
        r = requests.post(url, data=data, timeout=25)
    else:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = {
            "chat_id": CHANNEL,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        r = requests.post(url, data=data, timeout=25)
    r.raise_for_status()
    return r.json()

# === Основная функция ===
def main():
    sources = load_sources()

    collected = []
    for src in sources:
        try:
            if src["type"] == "rss":
                collected += collect_rss(src)
            elif src["type"] == "html":
                collected += collect_html(src)
        except Exception as ex:
            print("collect error:", src.get("name"), ex)

    if not collected:
        print("No items collected")
        return

    items = [it for it in collected if is_future(it.get("start"))]
    items = dedupe(items)

    def keyf(it):
        s = it.get("start")
        return dtparser.isoparse(s) if s else datetime.now(TZ) + timedelta(days=365)
    items.sort(key=keyf)

    state = load_state()
    seen = set(state.get("posted_ids", []))

    posted = 0
    for it in items[:MAX_POSTS]:
        if it["id"] in seen:
            continue
        try:
            post_message(format_post(it), it.get("image"))
            seen.add(it["id"])
            posted += 1
            time.sleep(1.2)
        except Exception as ex:
            print("post error:", ex)

    print(f"Posted: {posted}")
    state["posted_ids"] = list(seen)
    save_state(state)

if __name__ == "__main__":
    main()
