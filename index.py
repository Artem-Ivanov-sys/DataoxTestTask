from dotenv import load_dotenv
from os import getenv
from aiohttp import ClientSession
from aiohttp.client_exceptions import ContentTypeError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from asyncpg import connect
from typing import List, Set, Dict
from itertools import batched
from datetime import datetime
import json
from re import findall
from pytz import timezone

MAX_PAGES_COUNT = 20

async def urls_parser(
        session: ClientSession,
        base_url: str,
        page: int
) -> List[str]:
    async with session.get(base_url+f"?page={page}") as response:
        # print(base_url+f"?page={page}")
        if response.status == 404 or page > MAX_PAGES_COUNT:
            return ["END"]
        else:
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            return [x["href"] for x in soup.find_all("a", class_="address") if "newauto" not in x["href"]]

async def get_urls(base_url: str, batch_size: int = 20) -> Set[str]:
    urls = set()
    ua = UserAgent()
    print("Start")

    async with ClientSession(headers={"User-Agent": ua.chrome}) as session:
        page = 0
        while "END" not in urls:
            workers = [
                asyncio.create_task(
                    urls_parser(
                        session, base_url, i
                    )
                ) for i in range(page, page + batch_size)
            ]
            try:
                results = await asyncio.gather(*workers)
            except:
                pass
            for result in results:
                urls.update(result)
            print(f"\r{len(urls)} urls                ", end="")
            page += batch_size
        print()
    urls.remove("END")
    print(f"::Got {len(urls)} pages")
    return urls

async def car_parser(
        session: ClientSession,
        url: str,
        i: int = 1
) -> None|Dict:
    async with session.get(url) as response:
        if response.status != 200:
            print(f"STATUS {response.status}")
            return
        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        
        try:
            await asyncio.sleep(1)
            phone = 0
            try:
                if "\"userId\"" in html:
                    payload_params = {
                        "popUpId": "autoPhone",
                        "autoId": int(findall(r"(\d+)\.html", url)[0]),
                        "isConfirmPhoneEmailRequired": False,
                        "params": {
                            "userId": int(findall(r"\"userId\":(\d+)", html)[0]),
                            "phoneId": findall(r"\"phoneId\":\"(\d+)", html)[0]
                        },
                        "langId": 4,
                        "device": "desktop-web"
                    }
                    phone_url = "https://auto.ria.com/bff/final-page/public/auto/popUp/"
                else:
                    payload_params = {
                        "adv_id": int(findall(r"(\d+)\.html", url)[0]),
                        "category_id": 1,
                        "ivr": 1,
                        "owner_id": int(findall(r"\"owner_id\":(\d+)", html)[0]),
                        "phone_id": findall(r"\"phone_id\":\"(\d+)", html)[0],
                        "platform": "desktop"
                    }
                    phone_url = "https://auto.ria.com/newauto/api/auth/dc/"
                    print(url)
            except IndexError: # deleted auto
                return
            async with session.post(phone_url, data=json.dumps(payload_params), headers={
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://auto.ria.com',
                    'priority': 'u=1, i',
                    'referer': 'https://auto.ria.com/uk/auto_mitsubishi_outlander_39271570.html',
                    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    'x-ria-source': 'vue3-1.40.0',
                    'Cookie': 'ab_redesign=1; ab_test_new_pages=1; bffState={}'
                }) as r:
                try:
                    phone = int("380" + (await r.json())["additionalParams"]["phoneStr"].replace(" ", "").replace("(", "").replace(")", "")) if phone_url == "https://auto.ria.com/bff/final-page/public/auto/popUp/" else int("380" + (await r.json())["phone"])
                    if phone is None:
                        print("PHONE IS NONE: ", (await r.json())["additionalParams"])
                except ContentTypeError:
                    return
            
            try:
                result = {
                    "url": url,
                    "title": soup.find("h1").text,
                    "price_usd": int(soup.find(id="basicInfoPrice").text.split("$")[0].replace("\xa0", "")),
                    "odometer": int(soup.find(id="basicInfoTableMainInfo0").text.split()[0])*1000,
                    "username": soup.find(id="sellerInfoUserName").text,
                    "phone_number": phone,
                    "image_url": soup.find(id="photoSlider").find("img")["data-src"],
                    "image_count": len(soup.find(id="photoSlider").find_all("li")),
                    "car_number": soup.find(id="badges").find(class_="car-number ua").text,
                    "car_vin": soup.find(id="badges").find(class_="badge-template").text,
                    "datetime_found": datetime.now()
                }
            except:
                return None
            return result if None not in result.values() else None
        except AttributeError as a:
            if "429" in html and "too many requests" in html.lower():
                print("retry")
                await asyncio.sleep(i ** 2)
                return await car_parser(session, url, i+1)
            return

async def get_records(base_url: str, batch_size: int = 10) -> None:
    urls = await get_urls(base_url)
    records: List[Dict] = []

    async with ClientSession() as session:
        count = 0
        for batch in batched(urls, batch_size):
            workers = [
                asyncio.create_task(
                    car_parser(session, i)
                ) for i in batch
            ]

            results = await asyncio.gather(*workers)
            for i in results:
                records.append(i)
            count += len(batch)
            print("\r["+(int(count / len(urls) * 20))*"="+(20-int(count / len(urls) * 20))*" "+f"]   {count} / {len(urls)}", end="")
            await asyncio.sleep(1)
        print()
    while None in records:
        records.remove(None)
    print(f"::Parsed {len(records)} records")

    DB_USER = getenv("POSTGRES_USER")
    DB_PASSWORD= getenv("POSTGRES_PASSWORD")
    DB_NAME = getenv("POSTGRES_DB")
    db = await connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host="postgres", port=5432)

    await db.execute(
        "TRUNCATE TABLE cars;"
    )
    
    await db.copy_records_to_table(
        "cars",
        records=[list(record.values()) for record in records],
        columns = ["url", "title", "price_usd", "odometer", "username", "phone_number", "image_url", "image_count", "car_number", "car_vin", "datetime_found"]
    )

    await db.close()
    print("::Data was written successfully!")

async def create_dump(dump_folder: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dump_file = f"{dump_folder}/dump_{ts}.json"

    DB_USER = getenv("POSTGRES_USER")
    DB_PASSWORD= getenv("POSTGRES_PASSWORD")
    DB_NAME = getenv("POSTGRES_DB")
    db = await connect(user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host="postgres", port=5432)

    records = await db.fetch(
        "SELECT * FROM cars;"
    )

    data = []
    for row in records:
        data.append(dict(row))
        data[-1]["datetime_found"] = data[-1]["datetime_found"].isoformat()

    with open(dump_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"::Dump created: {dump_file}")

async def main() -> None:
    load_dotenv()

    BASE_URL = "https://auto.ria.com/uk/car/used/"
    TIME = getenv("SCHEDULED_TIME")
    DUMB_TIME = getenv("DUMP_TIME")
    DUMP_FOLDER = "dumps"

    scheduler = AsyncIOScheduler(
        timezone=timezone("Europe/Kyiv")
    )
    scheduler.add_job(get_records, "cron", hour=int(TIME.split(":")[0]), minute=int(TIME.split(":")[1]), args=[BASE_URL])
    scheduler.add_job(create_dump, "cron", hour=int(DUMB_TIME.split(":")[0]), minute=int(DUMB_TIME.split(":")[1]), args=[DUMP_FOLDER])
    scheduler.start()
    await get_records(BASE_URL)
    await create_dump(DUMP_FOLDER)
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    print("::Script running")
    asyncio.run(main())
