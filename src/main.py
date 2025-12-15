import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import base64
from urllib.parse import urljoin

BASE_URL = "https://www.epey.com"
LIST_BASE = "https://www.epey.com/akilli-telefonlar"

# =========================
# Cloudflare-aware scraper
# =========================
scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "desktop": True
    }
)

# =========================
# Popülerlik URL üretici
# =========================
def build_sort_url(sort_value: str) -> str:
    payload = f'N;_s:{len(sort_value)}:"{sort_value}";'
    encoded = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
    return f"{LIST_BASE}/e/{encoded}/"


# =========================
# Sayfalı popüler ürünler
# =========================
def get_popular_products(limit=100):
    sort_url = build_sort_url("tiklama:DESC")
    products = []
    page = 1

    while len(products) < limit:
        page_url = sort_url if page == 1 else f"{sort_url}{page}/"
        print(f"📄 Sayfa çekiliyor: {page_url}")

        response = scraper.get(
            page_url,
            headers={"Referer": LIST_BASE},
            timeout=30
        )

        if response.status_code != 200:
            print("⛔ Sayfa erişilemedi, durduruluyor.")
            break

        soup = BeautifulSoup(response.text, "lxml")
        rows = soup.select("ul.metin.row")

        if not rows:
            print("⛔ Ürün kalmadı, durduruluyor.")
            break

        for ul in rows:
            name_el = ul.select_one("a.urunadi")
            if not name_el:
                continue

            products.append({
                "urun_adi": name_el.get_text(strip=True),
                "url": urljoin(BASE_URL, name_el["href"])
            })

            if len(products) >= limit:
                break

        page += 1
        time.sleep(random.uniform(1.0, 1.8))  # insan davranışı

    return products


# =========================
# Ürün detayları
# =========================
def get_product_detail(product_url: str):
    response = scraper.get(
        product_url,
        headers={"Referer": LIST_BASE},
        timeout=30
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    data = {}

    h1 = soup.select_one("h1")
    data["urun_adi"] = h1.get_text(strip=True) if h1 else ""

    price = soup.select_one(".fiyat")
    data["fiyat"] = price.get_text(" ", strip=True) if price else ""

    for row in soup.select("div.ozellikler table tr"):
        cols = row.find_all("td")
        if len(cols) == 2:
            key = (
                cols[0]
                .get_text(strip=True)
                .lower()
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
            )
            val = cols[1].get_text(" ", strip=True)
            data[key] = val

    data["url"] = product_url
    return data


# =========================
# MAIN
# =========================
def main():
    print("🚀 Popüler ürünler (sayfalı) çekiliyor...")
    products = get_popular_products(limit=100)
    print(f"✅ Toplam {len(products)} popüler ürün alındı")

    all_data = []

    for i, p in enumerate(products, start=1):
        print(f"[{i}/{len(products)}] {p['urun_adi']}")
        try:
            detail = get_product_detail(p["url"])
            all_data.append(detail)
            time.sleep(random.uniform(1.2, 2.5))
        except Exception as e:
            print("❌ Hata:", p["url"], e)

    df = pd.DataFrame(all_data)
    df.to_csv(
        "epey_populer_akilli_telefonlar.csv",
        index=False,
        encoding="utf-8-sig"
    )

    print("🎉 CSV hazır: epey_populer_akilli_telefonlar.csv")

if __name__ == "__main__":
    main()