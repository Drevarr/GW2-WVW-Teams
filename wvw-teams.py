import configparser
from datetime import datetime, timezone
import os
import time
from typing import List
import requests
import pandas as pd
import urllib.error


MAX_FIELD_CHARS = 1024

def fetch_guild_data(cache_dir: str = "cache", ttl: int = 3600, retries: int = 3, delay: float = 2.0):
    """
    Fetches data from the 'Alliances' and 'SoloGuilds' worksheets of the WvW Guilds Google Spreadsheet.
    Always uses a local cache to avoid hitting Google unnecessarily.
    
    - Alliances: only columns A, C, V, W are kept.
    - SoloGuilds: only columns A, S, V are kept.
    """
    os.makedirs(cache_dir, exist_ok=True)

    sheet_id = "1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4"
    sheets = {
        "Alliances": "1294524185",
        "SoloGuilds": "2062340450"
    }
    urls = {
        name: f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        for name, gid in sheets.items()
    }

    results = {}
    for name, url in urls.items():
        cache_file = os.path.join(cache_dir, f"{name}.csv")

        # Check if cached version is valid
        if os.path.exists(cache_file) and time.time() - os.path.getmtime(cache_file) < ttl:
            df = pd.read_csv(cache_file)
        else:
            # Fetch fresh data with retries
            for attempt in range(1, retries + 1):
                try:
                    df = pd.read_csv(url)
                    df.to_csv(cache_file, index=False)
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 400 and attempt < retries:
                        time.sleep(delay * attempt)  # exponential backoff
                    else:
                        raise  # re-raise if retries exhausted

        # Keep only selected columns
        if name == "Alliances":
            keep_cols = [0, 2, 21, 22]  # A, C, V, W
            df = df.iloc[:, keep_cols]
        elif name == "SoloGuilds":
            keep_cols = [0, 18, 21]  # A, S, V
            df = df.iloc[:, keep_cols]

        results[name] = df

    return results["Alliances"], results["SoloGuilds"]

def fetch_guild_data_local(alliances_file: str = "WvW Guilds - Alliances.csv",
                           solo_file: str = "WvW Guilds - SoloGuilds.csv"):
    """
    Fetches data from locally exported CSV copies of the WvW Guilds spreadsheet.

    - Alliances: only columns A, C, V, W are kept.
    - SoloGuilds: only columns A, S, V are kept.
    """
    alliances = pd.read_csv(alliances_file, usecols=[0, 2, 21, 22], encoding='utf-8')  # A, C, V, W
    solo_guilds = pd.read_csv(solo_file, usecols=[0, 18, 21], encoding='utf-8')        # A, S, V

    return alliances, solo_guilds


def build_guild_embeds(world_name: str, alliances: pd.DataFrame, solo_guilds: pd.DataFrame) -> List[dict]:
    """Build embeds for a world, auto-splitting alliances if too long."""
    alliance_blocks = []
    for index, row in alliances.iterrows():
        block_lines = [f"**{row['Alliance:']}**"]  # alliance name
        alliance_guild_list = row['Guilds'].split("\n")
        for guild in alliance_guild_list:
            block_lines.append(f"-  {guild}")
        alliance_blocks.append("\n".join(block_lines))

    embeds, current_alliances, current_length = [], [], 0
    for block in alliance_blocks:
        block_len = len(block) + 2
        if current_length + block_len > MAX_FIELD_CHARS and current_alliances:
            embeds.append(make_embed(world_name, "\n\n".join(current_alliances)))
            current_alliances, current_length = [], 0
        current_alliances.append(block)
        current_length += block_len

    if current_alliances:
        embeds.append(make_embed(world_name, "\n\n".join(current_alliances)))

    # Attach solo guilds to last embed
    if embeds:
        world_solo_guilds = []
        for solo_index, solo_row in solo_guilds.iterrows():
            if solo_row['World'] == world_name:
                world_solo_guilds.append(solo_row['Solo Guilds'])
        solo_text = "\n".join(world_solo_guilds) if world_solo_guilds else "None"
        embeds[-1]["fields"].append({"name": "__Solo Guilds__", "value": solo_text})

    return embeds


def make_embed(world_name: str, alliance_text: str) -> dict:
    """Base embed template."""
    return {
        "author": {
            "name": "WvW Teams",
            "icon_url": "https://avatars.githubusercontent.com/u/16168556?v=4",
            "url": "https://github.com/Drevarr"
        },
        "title": f"{world_name} Guild List",
        "fields": [{"name": "__**Alliances**__", "value": alliance_text}],
        "thumbnail": {
            "url": "https://cdn.discordapp.com/attachments/1198282509960618025/1198282510770122853/Alliance_Logo_NA_UPDATED3.png"
        },
        "footer": {"text": "Last Updated:"},
        "timestamp": datetime.now(timezone.utc).isoformat() 
    }


def safe_post(webhook_url, payload):
    while True:
        resp = requests.post(webhook_url, json=payload)
        if resp.status_code == 429:
            retry_after = resp.json().get("retry_after", 1) / 1000.0
            print(f"Rate limited, retrying in {retry_after:.2f}s")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp
    

def post_embeds_and_get_links(webhook_url: str, guild_id: str, world_name: str, embeds: list) -> str:
    """
    Post embeds for one world and return the *first* message link.
    If multiple embeds are posted, only the first link is returned.
    """
    if not webhook_url.endswith("?wait=true"):
        webhook_url = webhook_url + "?wait=true"
            
    first_link = None
    for embed in embeds:
        resp = requests.post(webhook_url, json={"embeds": [embed]})
        data = resp.json()
        message_id = data["id"]
        channel_id = data["channel_id"]
        jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
        if not first_link:
            first_link = jump_url
    return first_link



def build_summary_embed(world_links: dict) -> dict:
    """Build a summary embed linking to the *first* message of each world."""
    lines = [f"[{world}]({link})" for world, link in world_links.items()]
    return {
        "title": "WvW Guild Lists Summary",
        "description": "\n".join(lines),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":

    config_ini = configparser.ConfigParser()
    config_ini.read("config.ini")

    WEBHOOK_URL = config_ini["Settings"]["WEBHOOK_URL"]
    GUILD_ID = config_ini["Settings"]["GUILD_ID"]
    
    world_links = {}

    alliances, solo_guilds = fetch_guild_data_local()
    clean_solo_guilds = solo_guilds.dropna()
    clean_alliances = alliances.dropna()
    sorted_alliances = clean_alliances.sort_values(by=['World ID', 'Alliance:'], ascending=[True, True])
    sorted_solo_guilds = clean_solo_guilds.sort_values(by=['World', 'Solo Guilds'], ascending=[True, True])
    world_list = sorted_alliances['World ID'].unique().tolist()

    #build_discord_embeds(sorted_alliances, sorted_solo_guilds)
    for world_name in world_list:
        filtered_alliances = sorted_alliances.loc[sorted_alliances['World ID'] == world_name]
        filtered_solo_guilds = sorted_solo_guilds.loc[sorted_solo_guilds['World'] == world_name]
        embeds = build_guild_embeds(world_name, sorted_alliances, sorted_solo_guilds)
        link = post_embeds_and_get_links(WEBHOOK_URL, GUILD_ID, world_name, embeds)
        world_links[world_name] = link
        time.sleep(0.5)

    # Post the summary embed
    summary = build_summary_embed(world_links)
    requests.post(WEBHOOK_URL, json={"embeds": [summary]})