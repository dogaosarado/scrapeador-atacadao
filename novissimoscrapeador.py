import aiohttp
import asyncio
import json

BASE_URL = "https://www.atacadao.com.br/api/io/_v/api/intelligent-search/product_search"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

OUTPUT_FILE = "atacadao_catalog.json"

CONCURRENT_REQUESTS = 5
PAGE_SIZE = 50


async def fetch_page(session, page):
    params = {
        "q": "",
        "from": page * PAGE_SIZE,
        "to": (page + 1) * PAGE_SIZE - 1
    }

    try:
        async with session.get(BASE_URL, headers=HEADERS, params=params) as response:
            if response.status != 200:
                return []

            data = await response.json()
            return data.get("products", [])

    except Exception:
        return []


def extract(product):
    try:
        items = product.get("items", [])

        skus = []

        for item in items:
            seller = item.get("sellers", [{}])[0]
            offer = seller.get("commertialOffer", {})

            skus.append({
                "sku": item.get("itemId"),
                "ean": item.get("ean"),
                "referenceId": item.get("referenceId"),
                "name": item.get("name"),

                "price": offer.get("Price"),
                "list_price": offer.get("ListPrice"),
                "price_without_discount": offer.get("PriceWithoutDiscount"),
                "reward_value": offer.get("RewardValue"),

                "available": offer.get("IsAvailable"),
                "available_quantity": offer.get("AvailableQuantity"),

                "installments": offer.get("Installments"),

                "seller": seller.get("sellerName"),
                "seller_id": seller.get("sellerId"),

                "images": [
                    img.get("imageUrl")
                    for img in item.get("images", [])
                ],

                "dimensions": item.get("measurementUnit"),
                "unit_multiplier": item.get("unitMultiplier")
            })

        return {
            "product_id": product.get("productId"),
            "product_name": product.get("productName"),
            "brand": product.get("brand"),
            "brand_id": product.get("brandId"),

            "link_text": product.get("linkText"),
            "url": f"https://www.atacadao.com.br/{product.get('linkText')}/p",

            "categories": product.get("categories"),
            "category_ids": product.get("categoryIds"),

            "description": product.get("description"),
            "meta_tag_description": product.get("metaTagDescription"),

            "release_date": product.get("releaseDate"),

            "cluster_highlights": product.get("clusterHighlights"),
            "product_clusters": product.get("productClusters"),

            "specification_groups": product.get("specificationGroups"),
            "properties": product.get("properties"),

            "items": skus,

            # keep raw product too (optional but powerful)
            "raw": product
        }

    except Exception:
        return None


async def scrape():
    results = []
    page = 0

    async with aiohttp.ClientSession() as session:

        while True:
            tasks = [
                fetch_page(session, page + i)
                for i in range(CONCURRENT_REQUESTS)
            ]

            pages = await asyncio.gather(*tasks)

            stop = True

            for products in pages:
                if products:
                    stop = False

                for p in products:
                    data = extract(p)
                    if data:
                        results.append(data)

            print(f"Scraped {len(results)} products")

            if stop:
                break

            page += CONCURRENT_REQUESTS

    # remove duplicates
    unique = {p["product_id"]: p for p in results}

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(list(unique.values()), f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    asyncio.run(scrape())
