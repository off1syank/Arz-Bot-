import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
from glob import glob
from queue import Empty, Queue

import requests
from requests.adapters import HTTPAdapter

# =========================
# CONFIG
# =========================
BOT_TOKEN = "твой"
CHAT_ID = "твой"
SITE_TOKEN = ""

CHECK_INTERVAL = 1.0
REQUEST_TIMEOUT = 2
MAX_RETRIES = 2
INITIAL_BACKOFF = 0.15
MAX_BACKOFF = 1.0

CACHE_FILE = "sent_cache.json"
LOG_FILE = "bot.log"
CLEAR_INTERVAL = 12 * 3600
MAPPINGS_DIR = "mappings"
ITEMS_JSON = "items_data26.json"

BUY_SERVER_FILTER = "vice city"
MIN_PROFIT_VC = 500_000
MAX_REPORTS_PER_RUN = 300
EXCHANGE_FEE = 0.05

IGNORE_ITEMS = [
    "Улучшенный сироп характеристик актера",
    "Видеокарта",
]
IGNORE_ITEMS = [i.lower() for i in IGNORE_ITEMS]

TELEGRAM_QUEUE = Queue(maxsize=5000)
TELEGRAM_WORKERS = 2
TELEGRAM_MAX_ATTEMPTS = 2
TELEGRAM_INITIAL_BACKOFF = 0.2
TELEGRAM_MAX_BACKOFF = 1.0
TELEGRAM_REQUEST_TIMEOUT = 1
TELEGRAM_MSG_TTL = 900

SERVER_MAP = {
    1: "Phoenix", 2: "Tucson", 3: "Scottdale", 4: "Chandler", 5: "Brainburg",
    6: "Saint-Rose", 7: "Mesa", 8: "Red-Rock", 9: "Yuma", 10: "Surprise",
    11: "Prescott", 12: "Glendale", 13: "Kingman", 14: "Winslow", 15: "Payson",
    16: "Gilbert", 17: "Show-Low", 18: "Casa-Grande", 19: "Page", 20: "Sun-City",
    21: "Queen-Creek", 22: "Sedona", 23: "Holiday", 24: "Wednesday", 25: "Yava",
    26: "Faraway", 27: "Bumble Bee", 28: "Christmas", 29: "Mirage", 30: "Love",
    31: "Drake", 32: "Space"
}

EXCHANGE_RATES = {
    "phoenix": {"buy": 112, "sell": 144},
    "tucson": {"buy": 155, "sell": 200},
    "scottdale": {"buy": 125, "sell": 162},
    "chandler": {"buy": 109, "sell": 139},
    "brainburg": {"buy": 106, "sell": 131},
    "saint rose": {"buy": 104, "sell": 136},
    "saint-rose": {"buy": 104, "sell": 136},
    "mesa": {"buy": 136, "sell": 176},
    "red rock": {"buy": 94, "sell": 134},
    "red-rock": {"buy": 94, "sell": 134},
    "yuma": {"buy": 110, "sell": 140},
    "surprise": {"buy": 94, "sell": 130},
    "prescott": {"buy": 93, "sell": 120},
    "glendale": {"buy": 93, "sell": 120},
    "kingman": {"buy": 93, "sell": 120},
    "winslow": {"buy": 93, "sell": 120},
    "payson": {"buy": 93, "sell": 120},
    "gilbert": {"buy": 93, "sell": 120},
    "show low": {"buy": 93, "sell": 120},
    "show-low": {"buy": 93, "sell": 120},
    "casa grande": {"buy": 93, "sell": 120},
    "casa-grande": {"buy": 93, "sell": 120},
    "page": {"buy": 93, "sell": 120},
    "sun city": {"buy": 93, "sell": 120},
    "sun-city": {"buy": 93, "sell": 120},
    "queen creek": {"buy": 93, "sell": 120},
    "queen-creek": {"buy": 93, "sell": 120},
    "sedona": {"buy": 93, "sell": 120},
    "holiday": {"buy": 93, "sell": 120},
    "wednesday": {"buy": 93, "sell": 120},
    "yava": {"buy": 93, "sell": 120},
    "faraway": {"buy": 93, "sell": 120},
    "bumble bee": {"buy": 93, "sell": 120},
    "christmas": {"buy": 93, "sell": 120},
    "mirage": {"buy": 93, "sell": 120},
    "love": {"buy": 93, "sell": 120},
    "drake": {"buy": 93, "sell": 120},
    "space": {"buy": 93, "sell": 120},
    "vice city": {"buy": 1, "sell": 1},
}

DIGITS_RE = re.compile(r"\D")
ITEM_RE = re.compile(r"^(\d+)(?:\(([^)]+)\))?$")
MULTISPACE_RE = re.compile(r"\s+")


def is_ignored_item(name: str) -> bool:
    return name.lower().strip() in IGNORE_ITEMS


def build_session(pool_size: int = 32) -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"Accept": "application/json", "Connection": "keep-alive"})
    if SITE_TOKEN:
        s.headers["Authorization"] = f"Bearer {SITE_TOKEN}"
    return s


SESSION = build_session(32)
TG_SESSION = build_session(16)

log_lock = threading.Lock()
cache_lock = threading.Lock()
last_cache_flush = 0.0
cache_dirty = False


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


@lru_cache(maxsize=256)
def normalize_server_name_raw(value) -> str:
    if not value:
        return ""
    s = str(value).lower().strip().replace("_", " ").replace("-", " ")
    return MULTISPACE_RE.sub(" ", s).strip()


def atomic_write(path: str, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def load_cache():
    sent = set()
    last = 0
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            raw_sent = data.get("sent", [])
            if isinstance(raw_sent, list):
                sent = set(str(x) for x in raw_sent)
            elif isinstance(raw_sent, dict):
                sent = set(str(x) for x in raw_sent.keys())
            last = int(data.get("last_cleared", 0))
        except Exception:
            sent = set()
            last = 0
    now = int(time.time())
    if now - last >= CLEAR_INTERVAL:
        sent = set()
        last = now
        atomic_write(CACHE_FILE, {"sent": list(sent), "last_cleared": last})
    return {"sent": sent, "last_cleared": last}


def mark_cache_dirty() -> None:
    global cache_dirty
    with cache_lock:
        cache_dirty = True


def flush_cache(cache_data, force: bool = False) -> None:
    global last_cache_flush, cache_dirty
    now = time.time()
    with cache_lock:
        if not force and (not cache_dirty or now - last_cache_flush < 2.0):
            return
        atomic_write(CACHE_FILE, {
            "sent": list(cache_data["sent"]),
            "last_cleared": int(cache_data["last_cleared"])
        })
        cache_dirty = False
        last_cache_flush = now


def load_mappings():
    mapping = {}
    try:
        if os.path.exists(ITEMS_JSON):
            with open(ITEMS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                mapping.update({str(k): str(v) for k, v in data.items()})
        if os.path.isdir(MAPPINGS_DIR):
            for path in glob(os.path.join(MAPPINGS_DIR, "*.json")):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        mapping.update({str(k): str(v) for k, v in data.items()})
                except Exception:
                    continue
    except Exception as e:
        log(f"Failed to load mappings: {e}")
    return mapping


MAPPING = load_mappings()


def map_id_to_name(item_id: int) -> str:
    return MAPPING.get(str(item_id)) or f"ID:{item_id}"


def format_money(amount) -> str:
    try:
        return f"{int(float(amount)):,}".replace(",", ".")
    except Exception:
        return str(amount)


def parse_item_entry(entry):
    s = str(entry)
    m = ITEM_RE.match(s)
    if m:
        return int(m.group(1)), m.group(2)
    digits = DIGITS_RE.sub("", s)
    return (int(digits), None) if digits else (None, None)


def parse_price_value(raw_price) -> int:
    if raw_price is None:
        return 0
    if isinstance(raw_price, int):
        return raw_price
    if isinstance(raw_price, float):
        return int(raw_price)
    digits = DIGITS_RE.sub("", str(raw_price))
    return int(digits) if digits else 0


def extract_shop_number(shop) -> str:
    if "LavkaUid" in shop:
        return str(shop.get("LavkaUid"))
    for key in ("marketplace_id", "id", "shopId", "number", "marketplaceId"):
        if key in shop:
            return str(shop.get(key))
    return ""


def extract_seller_nick(shop) -> str:
    username = shop.get("username")
    if isinstance(username, str) and username.strip():
        return username
    for key in ("userLogin", "nick", "login", "nickname", "seller", "user"):
        value = shop.get(key)
        if isinstance(value, str) and value.strip():
            return value
        if isinstance(value, dict):
            nick = value.get("login") or value.get("nick") or value.get("username")
            if nick:
                return str(nick)
    return ""


def try_extract_price_for_item(shop, idx, item_id):
    for key in ("price_sell", "priceSell", "items_price", "prices", "items_sell_price"):
        values = shop.get(key)
        if isinstance(values, list) and idx < len(values):
            return values[idx]
    prices_map = shop.get("prices_map")
    if isinstance(prices_map, dict):
        return prices_map.get(str(item_id), prices_map.get(item_id))
    return None


def try_extract_buy_price_for_item(shop, idx, item_id):
    for key in ("price_buy", "priceBuy", "items_buy_price", "buy_prices", "prices_buy"):
        values = shop.get(key)
        if isinstance(values, list) and idx < len(values):
            return values[idx]
    prices_map = shop.get("prices_map_buy")
    if isinstance(prices_map, dict):
        return prices_map.get(str(item_id), prices_map.get(item_id))
    return None


def try_extract_count_for_item(shop, idx, item_id, sell=True) -> int:
    keys = (
        ["count_sell", "countSell", "items_count", "items_count_sell", "count", "countSellList"]
        if sell else
        ["count_buy", "countBuy", "items_count_buy", "buy_count", "countBuyList"]
    )
    for key in keys:
        values = shop.get(key)
        if isinstance(values, list) and idx < len(values):
            try:
                return int(values[idx])
            except Exception:
                pass
    for key in keys:
        if key in shop:
            try:
                return int(shop.get(key))
            except Exception:
                pass
    return 1


def detect_server_from_shop(shop) -> str:
    for key in ("st_name", "serverName", "server", "server_id", "serverId"):
        value = shop.get(key)
        if not value:
            continue
        if isinstance(value, dict):
            name = value.get("name") or value.get("serverName") or value.get("st_name")
            if name:
                return str(name)
        try:
            sid = int(value)
            return SERVER_MAP.get(sid, "Vice City")
        except Exception:
            digits = DIGITS_RE.sub("", str(value))
            if digits:
                return SERVER_MAP.get(int(digits), "Vice City")
            return str(value)
    return "Vice City"


def get_rate_for_server(server_name: str):
    return EXCHANGE_RATES.get(normalize_server_name_raw(server_name))


def convert_price_to_vc(price, server_from) -> int:
    p = parse_price_value(price)
    if p <= 0:
        return 0
    sname = normalize_server_name_raw(server_from)
    if not sname or sname == "vice city":
        return p
    s_rate = get_rate_for_server(sname)
    if not s_rate:
        return p
    s_buy = s_rate.get("buy")
    if not s_buy:
        return p
    denom = s_buy * ((1 - EXCHANGE_FEE) ** 2)
    if denom <= 0:
        return p
    return int(p / denom)


def safe_request(method, url, *, params=None, data=None, json_data=None, timeout=REQUEST_TIMEOUT, session=None):
    backoff = INITIAL_BACKOFF
    sess = session or SESSION
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return sess.request(method=method, url=url, params=params, data=data, json=json_data, timeout=timeout)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                log(f"Request error attempt={attempt} url={url}: {e}")
        except Exception as e:
            if attempt == MAX_RETRIES:
                log(f"Unexpected request error attempt={attempt} url={url}: {e}")
        if attempt == MAX_RETRIES:
            return None
        time.sleep(min(backoff, MAX_BACKOFF))
        backoff *= 2
    return None


def _send_telegram_once(text: str, reply_markup=None) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log("BOT_TOKEN or CHAT_ID is empty; telegram send skipped")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    if reply_markup is not None:
        payload["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)

    r = safe_request(
        "POST",
        url,
        data=payload,
        timeout=TELEGRAM_REQUEST_TIMEOUT,
        session=TG_SESSION
    )
    return r is not None and getattr(r, "status_code", None) == 200


def send_telegram(text: str, reply_markup=None) -> None:
    try:
        TELEGRAM_QUEUE.put_nowait({
            "text": text,
            "reply_markup": reply_markup,
            "ts": time.time(),
            "attempts": 0
        })
    except Exception:
        log("Telegram queue is full; message dropped")


def telegram_worker(worker_id: int) -> None:
    while True:
        try:
            msg = TELEGRAM_QUEUE.get(timeout=0.25)
        except Empty:
            continue
        try:
            if time.time() - msg.get("ts", time.time()) > TELEGRAM_MSG_TTL:
                continue
            attempts = msg.get("attempts", 0)
            backoff = TELEGRAM_INITIAL_BACKOFF
            success = False
            while attempts < TELEGRAM_MAX_ATTEMPTS:
                if _send_telegram_once(msg["text"], msg.get("reply_markup")):
                    success = True
                    break
                attempts += 1
                msg["attempts"] = attempts
                time.sleep(min(backoff, TELEGRAM_MAX_BACKOFF))
                backoff *= 2
            if not success:
                log(f"Telegram worker {worker_id}: failed after {attempts} attempts")
        finally:
            TELEGRAM_QUEUE.task_done()


def build_best_vc_buy_map(shops):
    best_by_item = {}
    for shop in shops:
        if not isinstance(shop, dict):
            continue
        server_name = detect_server_from_shop(shop)
        if normalize_server_name_raw(server_name) != BUY_SERVER_FILTER:
            continue
        items_buy = shop.get("items_buy") or shop.get("items_buy_list") or []
        if not items_buy:
            continue
        shop_num = extract_shop_number(shop)
        seller = extract_seller_nick(shop)
        for idx, entry in enumerate(items_buy):
            item_id, ench = parse_item_entry(entry)
            if not item_id:
                continue
            price = parse_price_value(try_extract_buy_price_for_item(shop, idx, item_id))
            if price <= 0:
                continue
            current = best_by_item.get(item_id)
            if current is None or price > current["price"]:
                best_by_item[item_id] = {
                    "item_id": item_id,
                    "name": map_id_to_name(item_id),
                    "price": price,
                    "count": try_extract_count_for_item(shop, idx, item_id, sell=False),
                    "shop_num": shop_num,
                    "nick": seller,
                    "server": server_name,
                    "ench": ench,
                }
    return best_by_item


def make_copy_button(command_text: str):
    return {
        "inline_keyboard": [
            [
                {
                    "text": f"Копировать {command_text}",
                    "copy_text": {
                        "text": command_text
                    }
                }
            ]
        ]
    }


def analyze_market_below_vc_buy(shops, cache_data):
    vc_buy_map = build_best_vc_buy_map(shops)
    if not vc_buy_map:
        return 0, 0, 0

    sell_rows = 0
    profitable = 0
    sent = 0
    local_cache_add = []

    sent_cache = cache_data["sent"]
    for shop in shops:
        if sent >= MAX_REPORTS_PER_RUN:
            break
        if not isinstance(shop, dict):
            continue

        server_name = detect_server_from_shop(shop)
        seller = extract_seller_nick(shop)
        shop_num = extract_shop_number(shop)
        items_sell = shop.get("items_sell") or shop.get("items") or shop.get("items_sell_list") or []
        if not items_sell:
            continue

        for idx, entry in enumerate(items_sell):
            if sent >= MAX_REPORTS_PER_RUN:
                break

            item_id, ench = parse_item_entry(entry)
            if not item_id:
                continue
            sell_rows += 1

            best_buy = vc_buy_map.get(item_id)
            if not best_buy:
                continue

            sell_price_native = parse_price_value(try_extract_price_for_item(shop, idx, item_id))
            if sell_price_native <= 0:
                continue

            sell_price_vc = convert_price_to_vc(sell_price_native, server_name)
            buy_price_vc = best_buy["price"]
            if sell_price_vc <= 0 or buy_price_vc <= 0 or sell_price_vc >= buy_price_vc:
                continue

            profit_vc = buy_price_vc - sell_price_vc
            if profit_vc < MIN_PROFIT_VC:
                continue

            profitable += 1
            count_sell = try_extract_count_for_item(shop, idx, item_id, sell=True)
            trade_volume = min(max(count_sell, 1), max(best_buy.get("count", 1), 1))
            total_profit_vc = profit_vc * trade_volume
            name = map_id_to_name(item_id)
            if is_ignored_item(name):
                continue

            cache_key = (
                f"vcbelow|{item_id}|{ench or ''}|{shop_num}|{server_name}|"
                f"{sell_price_native}|{best_buy['shop_num']}|{best_buy['price']}"
            )
            if cache_key in sent_cache:
                continue

            ench_text = f"\n✨ Заточка: {ench}" if ench else ""
            find_command = f"/findilavka {shop_num}"

            msg = (
                f"💰 Ниже скупа Vice City\n\n"
                f"📦 {name}{ench_text}\n"
                f"🌍 Сервер продажи: {server_name}\n"
                f"🏪 Лавка продажи: {find_command} | {seller or '-'}\n"
                f"💵 Цена продажи: {format_money(sell_price_native)} $\n"
                f"🪙 Цена продажи в VC: {format_money(sell_price_vc)} VC\n\n"
                f"🏙️ Скупка Vice City: {format_money(buy_price_vc)} VC\n"
                f"🏪 Лавка скупа: {best_buy['shop_num']} | {best_buy['nick'] or '-'}\n"
                f"📦 Объём сделки: {trade_volume}\n"
                f"📈 Профит за 1 шт: {format_money(profit_vc)} VC\n"
                f"🎁 Общий профит: {format_money(total_profit_vc)} VC"
            )

            reply_markup = make_copy_button(find_command)
            send_telegram(msg, reply_markup=reply_markup)

            local_cache_add.append(cache_key)
            sent += 1

    if local_cache_add:
        sent_cache.update(local_cache_add)
        mark_cache_dirty()

    return sell_rows, profitable, sent


def process_marketplace(cache_data):
    url = "https://api.arz.market/api/getSelectedMarketplace/-1"
    r = safe_request("GET", url, timeout=REQUEST_TIMEOUT, session=SESSION)
    if r is None:
        return False
    if getattr(r, "status_code", None) != 200:
        log(f"Marketplace returned status {getattr(r, 'status_code', 'unknown')}")
        return False

    try:
        data = r.json()
    except Exception as e:
        log(f"Failed to parse marketplace JSON: {e}")
        return False

    if isinstance(data, list):
        shops = data
    elif isinstance(data, dict):
        shops = data.get("data") or data.get("shops") or data.get("marketplaces") or data.get("items") or []
    else:
        shops = []

    if not isinstance(shops, list):
        log("Marketplace response did not contain a shop list")
        return False

    started = time.perf_counter()
    sell_rows, profitable, sent = analyze_market_below_vc_buy(shops, cache_data)
    elapsed = time.perf_counter() - started
    log(f"Scan complete: sell_rows={sell_rows}, profitable={profitable}, sent={sent}, took={elapsed:.3f}s")
    return True


def main():
    log("Ultra-fast bot started")
    cache = load_cache()

    for i in range(TELEGRAM_WORKERS):
        threading.Thread(target=telegram_worker, args=(i + 1,), daemon=True).start()

    try:
        while True:
            loop_started = time.perf_counter()
            now = int(time.time())

            if now - cache["last_cleared"] >= CLEAR_INTERVAL:
                cache["sent"] = set()
                cache["last_cleared"] = now
                mark_cache_dirty()

            try:
                process_marketplace(cache)
                flush_cache(cache)
            except Exception as e:
                log(f"Unhandled error in processing loop: {e}")

            elapsed = time.perf_counter() - loop_started
            sleep_for = CHECK_INTERVAL - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                time.sleep(0.01)

    except KeyboardInterrupt:
        flush_cache(cache, force=True)
        log("Bot stopped by user")
    except Exception as e:
        flush_cache(cache, force=True)
        log(f"Fatal error: {e}")


if __name__ == "__main__":
    main()