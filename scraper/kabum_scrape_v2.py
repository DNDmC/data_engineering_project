import re
import io
import time
import random
import argparse
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15"
)


# =========================================================
# Azure Blob helpers
# =========================================================

def _bsc(conn_str: str):
    from azure.storage.blob import BlobServiceClient
    return BlobServiceClient.from_connection_string(conn_str)


def _list_blobs(conn_str: str, container: str, prefix: str) -> List[Tuple[str, int]]:
    bsc = _bsc(conn_str)
    cc = bsc.get_container_client(container)
    out = []
    for b in cc.list_blobs(name_starts_with=prefix):
        out.append((b.name, int(b.size or 0)))
    return out


def _download_blob_bytes(conn_str: str, container: str, blob_path: str) -> Tuple[bytes, int]:
    bsc = _bsc(conn_str)
    blob = bsc.get_blob_client(container=container, blob=blob_path)
    props = blob.get_blob_properties()
    size = int(props.size or 0)
    data = blob.download_blob().readall()
    return data, size


def _upload_blob_bytes(conn_str: str, container: str, blob_path: str, data: bytes):
    bsc = _bsc(conn_str)
    blob = bsc.get_blob_client(container=container, blob=blob_path)
    blob.upload_blob(data, overwrite=True)
    print(f"✅ Uploaded: {container}/{blob_path} ({len(data)} bytes)")


def _pick_csv_in_prefix(conn_str: str, container: str, prefix: str) -> str:
    """
    prefix: "inbox/kabum/gold_enrichment_v1"
    Procura um CSV válido dentro dele e retorna o caminho do blob escolhido.
    """
    p = prefix.rstrip("/") + "/"
    items = _list_blobs(conn_str, container, p)

    # pega CSVs não vazios
    csvs = [(name, size) for name, size in items if name.lower().endswith(".csv") and size > 0]
    if csvs:
        csvs.sort(key=lambda x: x[1], reverse=True)
        chosen = csvs[0][0]
        print(f"✅ Encontrado CSV no prefixo: {container}/{chosen}")
        return chosen

    # fallback: pega qualquer arquivo não vazio
    non_empty = [(name, size) for name, size in items if size > 0]
    if non_empty:
        non_empty.sort(key=lambda x: x[1], reverse=True)
        chosen = non_empty[0][0]
        print(f"⚠️ Não achei .csv; usando maior arquivo não-vazio: {container}/{chosen}")
        return chosen

    raise RuntimeError(
        f"Prefixo/pasta '{container}/{p}' não contém arquivo válido (tudo 0 bytes). "
        f"Passe o arquivo completo, ex: inbox/kabum/gold_enrichment_v1/gold_enrichment_v1.csv"
    )


def download_csv_from_azure(conn_str: str, container: str, blob_or_prefix: str) -> pd.DataFrame:
    """
    Aceita:
      - caminho de arquivo CSV real
      - OU prefixo/pasta (com um blob 0 bytes) e procura um CSV dentro
    """
    # 1) tenta baixar exatamente o que foi passado
    data, size = _download_blob_bytes(conn_str, container, blob_or_prefix)
    print(f"📦 Input blob: {container}/{blob_or_prefix} | size={size} bytes")

    # Se for 0 bytes ou whitespace, assume que é prefixo e procura um CSV dentro
    if data is None or len(data) == 0 or len(data.strip()) == 0:
        print("⚠️ Blob vazio/0 bytes. Vou tratar como prefixo e procurar um CSV dentro...")
        chosen = _pick_csv_in_prefix(conn_str, container, blob_or_prefix)
        data2, size2 = _download_blob_bytes(conn_str, container, chosen)
        print(f"📦 Chosen blob: {container}/{chosen} | size={size2} bytes")
        if data2 is None or len(data2) == 0 or len(data2.strip()) == 0:
            raise RuntimeError(f"Arquivo escolhido está vazio: {container}/{chosen}")
        return pd.read_csv(io.BytesIO(data2))

    # 2) tenta ler o CSV direto
    try:
        return pd.read_csv(io.BytesIO(data))
    except pd.errors.EmptyDataError:
        # se o conteúdo não for CSV, tenta como prefixo
        print("⚠️ Conteúdo não parseou como CSV. Vou tentar como prefixo...")
        chosen = _pick_csv_in_prefix(conn_str, container, blob_or_prefix)
        data2, size2 = _download_blob_bytes(conn_str, container, chosen)
        print(f"📦 Chosen blob: {container}/{chosen} | size={size2} bytes")
        return pd.read_csv(io.BytesIO(data2))


# =========================================================
# Scraping helpers
# =========================================================

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"})
    return s


def _safe_float(text: str) -> Optional[float]:
    if text is None:
        return None
    t = str(text).replace("\xa0", " ").strip()
    m = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", t)
    if m:
        try:
            return float(m.group(1).replace(".", "").replace(",", "."))
        except:
            return None
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except:
        return None


def _extract_resolution(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.lower()
    m = re.search(r"(\d{3,4})\s*[x×]\s*(\d{3,4})", t)
    if m:
        return f"{m.group(1)}x{m.group(2)}"
    if "full hd" in t or "fhd" in t:
        return "1920x1080"
    if "quad hd" in t or "qhd" in t:
        return "2560x1440"
    if "4k" in t or "uhd" in t or "ultra hd" in t:
        return "3840x2160"
    return None


def _extract_hz(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d{2,3})\s*hz", text.lower())
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None


def _extract_nits(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d{2,4})\s*nits", text.lower())
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None


def _extract_panel_type(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.lower()
    for p in ["ips", "va", "tn", "oled", "amoled", "wva"]:
        if re.search(rf"\b{p}\b", t):
            return p.upper()
    return None


def _extract_panel_finish(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.lower()
    if "antirreflex" in t or "anti-reflex" in t or "anti reflex" in t:
        return "ANTI-REFLEXO"
    if "fosco" in t:
        return "FOSCO"
    if "brilh" in t or "gloss" in t:
        return "BRILHANTE"
    return None


def scrape_kabum_product(url: str, session: requests.Session, timeout: int = 25, max_retries: int = 3) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")

            html = r.text
            soup = BeautifulSoup(html, "html.parser")
            page_text = soup.get_text(" ", strip=True)

            out = {
                "refresh_rate_hz": _extract_hz(page_text),
                "screen_resolution": _extract_resolution(page_text),
                "panel_type": _extract_panel_type(page_text),
                "panel_finish": _extract_panel_finish(page_text),
                "brightness_nits": _extract_nits(page_text),
            }

            price_val = None
            old_price_val = None
            money = re.findall(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}", html)
            if money:
                price_val = _safe_float(money[0])
                if len(money) >= 2:
                    old_price_val = _safe_float(money[1])

            out["price"] = price_val
            out["old_price"] = old_price_val

            out["scrape_ok"] = True
            out["scrape_error"] = None
            return out

        except Exception as e:
            last_err = e
            sleep_s = 0.7 + random.random() * 1.3 + (attempt - 1) * 0.8
            time.sleep(sleep_s)

    return {
        "refresh_rate_hz": None,
        "screen_resolution": None,
        "panel_type": None,
        "panel_finish": None,
        "brightness_nits": None,
        "price": None,
        "old_price": None,
        "scrape_ok": False,
        "scrape_error": str(last_err),
    }


# =========================================================
# Argparse (compatível com os flags antigos + novos)
# =========================================================

def parse_args():
    p = argparse.ArgumentParser(description="KaBuM - scrape specs using input CSV in Azure Blob")

    p.add_argument("--azure-conn-str", dest="azure_conn_str", default="", help="Azure Storage connection string")

    # ---------
    # FLAGS NOVOS 
    # ---------
    p.add_argument("--input-container", default="", help="Container do input CSV (ex: gold)")
    p.add_argument("--input-blob", default="", help="Blob do input CSV (ou prefixo) dentro do container")

    p.add_argument("--output-container", default="", help="Container do output CSV (default=input-container)")
    p.add_argument("--output-blob", default="", help="Blob do output CSV (default=derivado do input)")

    # ---------
    # FLAGS ANTIGOS 
    # ---------
    p.add_argument("--gold-container", default="", help="(LEGACY) Container do input (ex: gold)")
    p.add_argument("--gold-prefix", default="", help="(LEGACY) Prefixo/pasta do input dentro do container")
    p.add_argument("--out-name", default="", help="(LEGACY) Nome do arquivo de output")
    p.add_argument("--ingestion-date", default="", help="(LEGACY) apenas informativo")

    # scraping controls
    p.add_argument("--sleep-min", type=float, default=0.3, help="sleep mínimo entre requests (segundos)")
    p.add_argument("--sleep-max", type=float, default=0.9, help="sleep máximo entre requests (segundos)")
    p.add_argument("--max-rows", type=int, default=0, help="Se > 0, limita quantidade de linhas do input")

    return p.parse_args()


def _derive_input_from_legacy(args) -> Tuple[str, str]:
    """
    Converte legacy args em container + blob/prefixo.
    legacy:
      --gold-container gold
      --gold-prefix kabum/enrichment
      (e geralmente o arquivo dentro é gold_enrichment_v1/gold_enrichment_v1.csv ou semelhante)

    Aqui a gente usa o prefixo como blob_or_prefix e deixa a função robusta achar o CSV.
    """
    container = args.gold_container.strip()
    prefix = args.gold_prefix.strip().lstrip("/")
    if not container or not prefix:
        raise RuntimeError("Legacy args precisam de --gold-container e --gold-prefix")


    if not prefix.startswith("inbox/"):
        prefix = "inbox/" + prefix

    return container, prefix


def _derive_output_from_legacy(args, input_prefix: str) -> str:
    """
    legacy output: usa --out-name dentro do mesmo prefixo do input, para manter sua estrutura.
    Ex: input_prefix = inbox/kabum/gold_enrichment_v1
        out-name = kabum_enriched.csv
        => inbox/kabum/gold_enrichment_v1/kabum_enriched.csv
    """
    name = (args.out_name or "").strip()
    if not name:
        name = "kabum_enriched.csv"
    base = input_prefix.rstrip("/")
    return f"{base}/{name}"


def _derive_output_default_from_new(input_blob: str) -> str:
    b = input_blob.rstrip("/")
    if b.lower().endswith(".csv"):
        return b[:-4] + "_scraped.csv"
    return b + "/scraped_output.csv"


def main():
    args = parse_args()

    if not args.azure_conn_str:
        raise RuntimeError("azure conn str vazio. Passe --azure-conn-str ou defina AZURE_STORAGE_CONNECTION_STRING.")

    # resolve input (novo vs legacy)
    if args.input_container and args.input_blob:
        in_container = args.input_container.strip()
        in_blob_or_prefix = args.input_blob.strip().lstrip("/")
    else:
        # legacy
        in_container, in_blob_or_prefix = _derive_input_from_legacy(args)

    # resolve output
    if args.output_container or args.output_blob:
        out_container = (args.output_container or in_container).strip()
        out_blob = (args.output_blob or _derive_output_default_from_new(in_blob_or_prefix)).strip().lstrip("/")
    else:
        # legacy output
        out_container = in_container
        out_blob = _derive_output_from_legacy(args, in_blob_or_prefix)

    print(f"⬇️ Download input CSV from Azure: {in_container}/{in_blob_or_prefix}")
    df_in = download_csv_from_azure(args.azure_conn_str, in_container, in_blob_or_prefix)

    if df_in is None or df_in.empty:
        raise RuntimeError("Input CSV carregou, mas está vazio (sem linhas).")

    if "product_url" not in df_in.columns:
        raise RuntimeError(f"input precisa ter coluna product_url. Colunas encontradas: {list(df_in.columns)}")

    if args.max_rows and args.max_rows > 0:
        df_in = df_in.head(args.max_rows).copy()

    # garante colunas alvo existirem
    expected_cols = [
        "refresh_rate_hz",
        "screen_resolution",
        "panel_type",
        "panel_finish",
        "brightness_nits",
        "price",
        "old_price",
        "scrape_ok",
        "scrape_error",
    ]
    for c in expected_cols:
        if c not in df_in.columns:
            df_in[c] = None

    sess = _session()

    rows_out = []
    for _, row in tqdm(df_in.iterrows(), total=len(df_in), desc="Scraping KaBuM"):
        url = str(row["product_url"]).strip()
        new = row.to_dict()

        if not url or url.lower() == "nan":
            new["scrape_ok"] = False
            new["scrape_error"] = "product_url vazio"
            rows_out.append(new)
            continue

        scraped = scrape_kabum_product(url, sess)

        # atualiza só se veio valor (senão mantém input)
        for k, v in scraped.items():
            if k in ("scrape_ok", "scrape_error"):
                new[k] = v
            else:
                if v is not None and str(v).strip() != "":
                    new[k] = v

        rows_out.append(new)

        time.sleep(args.sleep_min + random.random() * max(0.0, args.sleep_max - args.sleep_min))

    df_out = pd.DataFrame(rows_out)

    # move scrape_ok/scrape_error pro final
    cols = list(df_out.columns)
    for c in ["scrape_ok", "scrape_error"]:
        if c in cols:
            cols.remove(c)
            cols.append(c)
    df_out = df_out[cols]

    csv_bytes = df_out.to_csv(index=False).encode("utf-8")
    print(f"⬆️ Upload output CSV to Azure: {out_container}/{out_blob}")
    _upload_blob_bytes(args.azure_conn_str, out_container, out_blob, csv_bytes)

    print("✅ Done.")


if __name__ == "__main__":
    main()