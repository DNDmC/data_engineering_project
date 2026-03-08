

import os
import re
import json
import time
import random
import hashlib
import argparse
from datetime import datetime
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient


UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15"
)
ACCEPT_LANG = "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"


# --------------------------
# Utils
# --------------------------
def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]+", "", s)
    return s


def now_utc_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def brl_to_float(v: str):
    """
    "4.599,90" -> 4599.90
    """
    if not v:
        return None
    v = v.replace("R$", "").strip()
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except Exception:
        return None


def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def build_product_key(product_id: str | None, url: str) -> str:
    if product_id:
        return f"kabum_{product_id}"
    return f"kabum_url_{sha1_short(url)}"


# --------------------------
# Azure upload
# --------------------------
def upload_bytes_to_azure(conn_str: str, container: str, blob_path: str, data: bytes):
    if not conn_str:
        raise RuntimeError("AZURE conn str não definida. Passe --azure-conn-str ou use env var.")

    bsc = BlobServiceClient.from_connection_string(conn_str)
    blob_client = bsc.get_blob_client(container=container, blob=blob_path)
    blob_client.upload_blob(data, overwrite=True)
    print(f"✅ Uploaded → {container}/{blob_path}")


# --------------------------
# HTTP with retry/backoff
# --------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": ACCEPT_LANG,
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s


def http_get(
    session: requests.Session,
    url: str,
    max_retries: int = 5,
    base_delay: float = 0.8,
    timeout: int = 30
) -> str:
    """
    Retry com exponential backoff + jitter.
    Trata 429/5xx e timeouts.
    """
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)

            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")

            r.raise_for_status()
            return r.text

        except Exception as e:
            last_err = e
            sleep_s = base_delay * (2 ** (attempt - 1))
            sleep_s = sleep_s * random.uniform(0.7, 1.3)
            sleep_s = min(sleep_s, 25)

            print(f"⚠️ GET failed ({attempt}/{max_retries}) url={url} err={e} -> sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"Falha após {max_retries} tentativas em {url}: {last_err}")


# --------------------------
# Parsing helpers
# --------------------------
def extract_product_id(url: str, soup: BeautifulSoup | None = None) -> str | None:
    # URL: /produto/<id>/
    m = re.search(r"/produto/(\d+)", url)
    if m:
        return m.group(1)

    # fallback: procura algum número grande no texto
    if soup:
        txt = soup.get_text(" ", strip=True)
        m2 = re.search(r"\b(\d{6,})\b", txt)
        if m2:
            return m2.group(1)

    return None


def extract_name(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)

    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    if soup.title:
        return soup.title.get_text(" ", strip=True)

    return None


def extract_price_fields(text: str) -> dict:
    """
    Retorna:
      list_price_brl -> preço cheio ("de")
      pix_price_brl  -> à vista no PIX
      card_price_brl -> no cartão/parcelado
      discount_pct_pix
    """
    raw_prices = re.findall(r"R\$\s?([\d\.]+,\d{2})", text)
    prices = [brl_to_float(p) for p in raw_prices]
    prices = [p for p in prices if p is not None]

    list_price = None
    pix_price = None
    card_price = None

    # PIX: pega o último preço antes de "À vista no PIX"
    if "À vista no PIX" in text:
        pix_anchor = text.index("À vista no PIX")
        before = text[:pix_anchor]
        raw_before = re.findall(r"R\$\s?([\d\.]+,\d{2})", before)
        if raw_before:
            pix_price = brl_to_float(raw_before[-1])

    # Cartão: preço antes de "em até"
    m_card = re.search(r"(R\$\s?[\d\.]+,\d{2}).{0,120}em até", text)
    if m_card:
        m_val = re.search(r"([\d\.]+,\d{2})", m_card.group(1))
        if m_val:
            card_price = brl_to_float(m_val.group(1))

    # Geral: maior costuma ser o "de", menor costuma ser PIX
    if prices:
        list_price = max(prices)
        if pix_price is None:
            pix_price = min(prices)
        if card_price is None:
            uniq = sorted(set(prices))
            if len(uniq) >= 3:
                card_price = uniq[1]
            elif len(uniq) == 2:
                card_price = uniq[1]

    discount_pct_pix = None
    if list_price and pix_price and list_price > 0 and list_price >= pix_price:
        discount_pct_pix = round((list_price - pix_price) / list_price * 100, 2)

    return {
        "list_price_brl": list_price,
        "pix_price_brl": pix_price,
        "card_price_brl": card_price,
        "discount_pct_pix": discount_pct_pix,
        "currency": "BRL",
    }


def extract_specs_kv(text: str) -> dict:
    """
    Captura pares "campo: valor" do bloco de especificações técnicas.
    Também guarda um dict inteiro em specs_kv.
    """
    t = text
    start = t.lower().find("especificações técnicas")
    if start != -1:
        t = t[start:]

    # tenta achar pares "chave: valor"
    pairs = re.findall(r"([a-zA-ZÀ-ú0-9 \-/()]+):\s*([^:]{1,220})", t)
    kv = {}

    for k, v in pairs:
        k2 = slugify(k).replace("__", "_")
        v2 = v.strip()
        if len(v2) > 300:
            v2 = v2[:300]
        if len(k2) < 2:
            continue
        kv[k2] = v2

    # mapeia alguns campos importantes
    out = {
        "cpu_model_kv": kv.get("processador") or kv.get("modelo_do_processador"),
        "ram_raw_kv": kv.get("memoria_ram"),
        "storage_raw_kv": kv.get("tamanho_do_ssd"),
        "screen_raw_kv": kv.get("resolucao_da_tela") or kv.get("polegadas_da_tela"),
        "gpu_raw_kv": kv.get("placa_de_video"),
        "os_kv": kv.get("sistema_operacional"),
        "brand_seller_kv": kv.get("vendido_e_entregue_por"),
        "specs_kv": kv,  # dict completo
    }
    return out


def extract_specs_from_text(text: str) -> dict:
    """
    Heurísticas por texto (nome + página).
    """
    t = text.lower()

    def m1(pattern, cast=None):
        m = re.search(pattern, t)
        if not m:
            return None
        val = m.group(1)
        if cast:
            try:
                return cast(val)
            except Exception:
                return None
        return val

    # RAM
    ram_gb = m1(r"\b(\d{1,2})\s?gb\b.*?(?:ram|ddr4|ddr5)?", int)

    # Storage
    storage_value = None
    storage_unit = None
    m = re.search(r"\b(ssd|nvme|m\.2)\b.*?\b(\d{3,4})\s?(gb)\b", t)
    if m:
        storage_value = int(m.group(2))
        storage_unit = "GB"
    else:
        m = re.search(r"\b(ssd|nvme|m\.2)\b.*?\b(\d)\s?(tb)\b", t)
        if m:
            storage_value = int(m.group(2))
            storage_unit = "TB"

    # Tela (15.6 / 15,6)
    screen_inches = None
    m = re.search(r"\b(\d{1,2}(?:[.,]\d)?)\s?\"\b", t)
    if m:
        try:
            screen_inches = float(m.group(1).replace(",", "."))
        except Exception:
            pass
    if screen_inches is None:
        m = re.search(r"\b(\d{1,2}(?:[.,]\d)?)\s?pol(?:\.|eg)?\b", t)
        if m:
            try:
                screen_inches = float(m.group(1).replace(",", "."))
            except Exception:
                pass

    # Resolução (1920x1080)
    screen_resolution = None
    m = re.search(r"\b(\d{3,4})\s?x\s?(\d{3,4})\b", t)
    if m:
        screen_resolution = f"{m.group(1)}x{m.group(2)}"

    # Refresh rate
    refresh_rate_hz = m1(r"\b(\d{2,3})\s?hz\b", int)

    # CPU
    cpu_brand = None
    if "intel" in t:
        cpu_brand = "Intel"
    elif "ryzen" in t or "amd" in t:
        cpu_brand = "AMD"

    cpu_model = None
    m = re.search(r"\b(i[3579])[-\s]?(\d{4,5}[a-z]{0,2})\b", t)
    if m:
        cpu_model = f"{m.group(1).upper()}-{m.group(2).upper()}"
    else:
        m = re.search(r"\bryzen\s?(\d)\s?(\d{4,5}[a-z]{0,2})\b", t)
        if m:
            cpu_model = f"Ryzen {m.group(1)} {m.group(2).upper()}"

    # GPU
    gpu = None
    if "rtx" in t:
        m = re.search(r"\brtx\s?(\d{4})\b", t)
        if m:
            gpu = f"RTX {m.group(1)}"
    elif "gtx" in t:
        m = re.search(r"\bgtx\s?(\d{3,4})\b", t)
        if m:
            gpu = f"GTX {m.group(1)}"
    elif "iris xe" in t:
        gpu = "Iris Xe"
    elif "radeon" in t:
        gpu = "Radeon"

    # OS
    os_name = None
    if "windows 11" in t:
        os_name = "Windows 11"
    elif "windows 10" in t:
        os_name = "Windows 10"
    elif "linux" in t:
        os_name = "Linux"

    # Brand (heurístico)
    brand = None
    brands = ["acer", "lenovo", "dell", "asus", "samsung", "hp", "positivo", "vaio", "avell", "apple", "msi", "gigabyte"]
    for b in brands:
        if re.search(rf"\b{re.escape(b)}\b", t):
            brand = b.upper() if b != "hp" else "HP"
            break

    return {
        "brand": brand,
        "ram_gb": ram_gb,
        "storage_value": storage_value,
        "storage_unit": storage_unit,
        "screen_inches": screen_inches,
        "screen_resolution": screen_resolution,
        "refresh_rate_hz": refresh_rate_hz,
        "cpu_brand": cpu_brand,
        "cpu_model": cpu_model,
        "gpu": gpu,
        "os": os_name,
    }


def extract_product_links_from_search(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    # a[href] com "/produto/"
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if "/produto/" in href:
            if href.startswith("/"):
                href = urljoin("https://www.kabum.com.br", href)
            links.add(href.split("?")[0])

    # fallback regex
    if not links:
        for m in re.findall(r'https?://www\.kabum\.com\.br/produto/\d+[^"\s]*', html):
            links.add(m.split("?")[0])

    return sorted(list(links))


def parse_product_page(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    name = extract_name(soup)

    # manter \n aqui ajuda MUITO no extractor de "campo: valor"
    text = soup.get_text("\n", strip=True)

    product_id = extract_product_id(url, soup)
    product_key = build_product_key(product_id, url)

    price_fields = extract_price_fields(text)

    # heurísticas do nome + texto
    specs = extract_specs_from_text((name or "") + " " + text)

    # KV do bloco de especificações
    kv_specs = extract_specs_kv(text)

    return {
        "marketplace": "kabum",
        "product_id": product_id,
        "product_key": product_key,
        "product_name": name,
        "product_url": url,
        **price_fields,
        **specs,
        **kv_specs,
        "scraped_at": now_utc_iso(),
    }


# --------------------------
# Scrape flow
# --------------------------
def scrape_term(session: requests.Session, term: str, max_pages: int) -> list[dict]:
    term_q = quote_plus(term)
    all_items = []
    seen = set()

    for page in range(1, max_pages + 1):
        search_url = f"https://www.kabum.com.br/busca/{term_q}?page_number={page}"
        print(f"🔎 Search: {search_url}")

        html = http_get(session, search_url)
        product_links = extract_product_links_from_search(html)

        print(f"  ↳ links encontrados: {len(product_links)}")
        if not product_links:
            break

        for url in product_links:
            if url in seen:
                continue
            seen.add(url)

            try:
                phtml = http_get(session, url)
                item = parse_product_page(url, phtml)
                item["search_term"] = term
                all_items.append(item)
            except Exception as e:
                all_items.append({
                    "marketplace": "kabum",
                    "search_term": term,
                    "product_url": url,
                    "error": str(e),
                    "scraped_at": now_utc_iso(),
                })

            time.sleep(random.uniform(0.5, 1.1))

        time.sleep(random.uniform(0.7, 1.4))

    return all_items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--terms", default="notebook,ultrabook")
    ap.add_argument("--max-pages", type=int, default=3)
    ap.add_argument("--ingestion-date", default=datetime.now().strftime("%Y-%m-%d"))

    ap.add_argument("--azure-conn-str", default=os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
    ap.add_argument("--bronze-container", default=os.getenv("AZ_BRONZE_CONTAINER", "bronze"))
    ap.add_argument("--bronze-prefix", default=os.getenv("AZ_BRONZE_PREFIX", "kabum/products"))

    args = ap.parse_args()

    terms = [t.strip() for t in args.terms.split(",") if t.strip()]
    session = make_session()

    for term in terms:
        term_slug = slugify(term)
        rows = scrape_term(session, term, args.max_pages)

        # JSONL
        payload = ("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n").encode("utf-8")

        blob_path = (
            f"{args.bronze_prefix}/"
            f"search_term={term_slug}/"
            f"ingestion_date={args.ingestion_date}/"
            f"data_{term_slug}.jsonl"
        )

        upload_bytes_to_azure(
            conn_str=args.azure_conn_str,
            container=args.bronze_container,
            blob_path=blob_path,
            data=payload
        )


if __name__ == "__main__":
    main()