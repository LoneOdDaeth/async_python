# import asyncio

# async def uzun_sureli_gorev():
#     print("işlem başladi")
#     await asyncio.sleep(5)
#     print("işlem sona erdi")

# async def ana_gorev():
#     print("ana görev başladi")
#     task = asyncio.create_task(uzun_sureli_gorev())
#     print("ana görev devam ediyor")
#     await asyncio.sleep(3)
#     print("ana görev tamamlandı")

# asyncio.run(ana_gorev())

import asyncio
import json
import aiohttp
import os
import aiofiles
from bs4 import BeautifulSoup

url = "https://bazaar.abuse.ch/downloads/misp/?C=M;O=D"

save_folder = "./json" 
json_log_path = os.path.join(save_folder, "last_file.txt") 
extract_path = os.path.join(save_folder, "extract.json") 

key_attributes = ["name", "md5", "sha256", "sha1", "sha3-384", "tlsh", "imphash", "ssdeep", "size-in-bytes", "mime-type", "filename"]

os.makedirs(save_folder, exist_ok=True)

async def write_last_downloaded_file(file_name):
    async with aiofiles.open(json_log_path, 'w', encoding='utf-8') as file:
        await file.write(file_name)

async def download_file(session, url, file_name, save_folder):
    file_url = url.rsplit('/', 1)[0] + '/' + file_name

    async with session.get(file_url) as response:
        file_path = os.path.join(save_folder, file_name)
        async with aiofiles.open(file_path, 'wb') as file:
            await file.write(await response.read())
        
    print(f"{file_name} downloaded")

async def get_latest_files(url, count, skip_file):
    """Fetch latest JSON files from the given URL asynchronously."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()

    soup = BeautifulSoup(html, "html.parser")
    links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.json')]

    if skip_file in links:
        links.remove(skip_file)

    return links[:count] if links else []

async def read_last_downloaded_file():
    if os.path.exists(json_log_path):
        async with aiofiles.open(json_log_path, 'r', encoding='utf-8') as file:
            return (await file.read()).strip()
        return None

async def save_extracted_data(extracted_data):
    async with aiofiles.open(extract_path, 'a', encoding='utf-8') as file:
        await file.write(json.dumps(extracted_data, ensure_ascii=False, indent=4))

async def process_json_file(json_file):
    json_file_path = os.path.join(save_folder, json_file)

    async with aiofiles.open(json_file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.loads(await file.read())
        except json.JSONDecodeError:
            print(f"Skipping {json_file}, invalid JSON format")
            return []

    extracted_entries = []

    if "Event" in data and "Object" in data["Event"]:
        print(f"Processing: {json_file}")

        for obj in data["Event"]["Object"]:
            extracted_entry = {"file_name": json_file}
            extracted_entry["name"] = obj.get("name", None)

            file_type = None
            tag_names = []

            for attribute in obj.get("Attribute", []):
                if attribute["type"] in key_attributes:
                    extracted_entry[attribute["type"]] = attribute["value"]

                    if attribute["type"] == "md5":
                        file_type = attribute.get("value", None)

                        if "Tag" in attribute and isinstance(attribute["Tag"], list):
                            tag_names = [tag.get("name") for tag in attribute["Tag"] if "name" in tag]
            
            extracted_entry["file_type"] = file_type
            extracted_entry["tag_name"] = tag_names if tag_names else None

            extracted_entries.append(extracted_entry)
    
    return extracted_entries

async def main():
    latest_files = await get_latest_files(url, 1, "manifest.json")
    last_downloaded_file = await read_last_downloaded_file()

    if last_downloaded_file and last_downloaded_file in latest_files:
        last_index = latest_files.index(last_downloaded_file)
        new_files = latest_files[:last_index]
    else:
        new_files = latest_files
    
    if new_files:
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*(download_file(session, url, file_name, save_folder) for file_name in reversed(new_files)))
        
        await write_last_downloaded_file(latest_files[0])
    else:
        print("No new files found.")
    
    files_to_process = new_files if new_files else []

    extracted_data = []
    if os.path.exists(extract_path):
        async with aiofiles.open(extract_path, 'r', encoding='utf-8') as file:
            try:
                extracted_data = json.loads(await file.read())
            except json.JSONDecodeError:
                extracted_data = []

    if files_to_process:
        results = await asyncio.gather(*(process_json_file(json_file) for json_file in files_to_process))
        for result in results:
            extracted_data.extend(result)

        await save_extracted_data(extracted_data)

asyncio.run(main())
