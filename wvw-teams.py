import argparse
import configparser
from datetime import datetime, timezone
import os
import sys
import time
from typing import List, Tuple, Dict, Union
import json
import urllib.error

import pandas as pd
import requests


MAX_FIELD_CHARS = 1024


def cache_data_file(data: Union[Dict[str, str], pd.DataFrame], cache_file: str) -> None:
    """Cache the data dictionary or dataframe to a JSON file."""
    if isinstance(data, pd.DataFrame):
        # store as records with meta info
        payload = {
            "_type": "dataframe",
            "data": data.to_dict(orient="records"),
            "columns": data.columns.tolist(),
        }
    elif isinstance(data, dict):
        payload = {"_type": "dict", "data": data}
    else:
        raise TypeError(f"Unsupported type {type(data)}")

    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_data_file(cache_file: str) -> Dict[str, str]:
    """Load the cached data dictionary or dataframe from JSON file."""
    if not os.path.exists(cache_file):
        return None

    with open(cache_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if payload["_type"] == "dataframe":
        return pd.DataFrame(payload["data"], columns=payload["columns"])
    elif payload["_type"] == "dict":
        return payload["data"]
    else:
        return payload["data"]
    

def fetch_north_american_guilds() -> dict:
    """
    Fetch the listing of North American Guilds from the Guild Wars 2 API.

    The GW2API is queried for the list of North American Guilds. The response is
    parsed as JSON and returned as a Python dictionary.

    Parameters:
        None

    Returns:
        dict: The list of North American Guilds as returned by the GW2API.
    """
    url = "https://api.guildwars2.com/v2/wvw/guilds/na"
    try:
        # Send a GET request to the GW2API with a timeout of 3.0 seconds
        response = requests.get(url, timeout=(3.0, 5))
        # Raise an exception for bad status codes
        response.raise_for_status()
        # Parse the JSON response
        data = response.json()
        return data
        
    except requests.exceptions.RequestException as error:
        # Print an error message if the request fails
        print(f"Error: {error}")
        return None
    

def detect_world_changes(prev_data: dict, curr_data: dict) -> dict:
    """
    Detect changes in world assignments, including new and missing guilds.

    Returns:
        dict with keys: 'changed', 'new', 'removed'
    """
    changes = {
        "changed": {},
        "new": {},
        "removed": {}
    }

    # Detect changed and new guilds
    for guild_id, new_world in curr_data.items():
        old_world = prev_data.get(guild_id)
        if old_world is None:
            changes["new"][guild_id] = new_world
        elif old_world != new_world:
            changes["changed"][guild_id] = {
                "from": old_world,
                "to": new_world
            }

    # Detect removed guilds
    for guild_id, old_world in prev_data.items():
        if guild_id not in curr_data:
            changes["removed"][guild_id] = old_world

    return changes


def fetch_match_data(match: str) -> dict:
    """
    Fetch match data for a given tier from GW2API.

    Parameters:
        match (str): The match ID to fetch data for. Example: '1-1'

    Returns:
        dict: The match data as returned by the GW2API. None if the request fails.
    """
    # Construct the URL for the match data request
    url = f'https://api.guildwars2.com/v2/wvw/matches/{match}'

    try:
        # Send a GET request to the GW2API with no timeout
        response = requests.get(url)
        # Raise an exception for bad status codes
        response.raise_for_status()
        # Parse the JSON response
        return response.json()
    except requests.exceptions.RequestException as error:
        # Print an error message if the request fails
        print(f"Error: {error}")
        return None


def fetch_guild_data(cache_dir: str = "cache", ttl: int = 3600, retries: int = 3, delay: float = 2.0):
    """
    Fetches data from the 'Alliances' and 'SoloGuilds' worksheets of the WvW Guilds Google Spreadsheet.
    Always uses a local cache to avoid hitting Google unnecessarily.
    
    - Alliances: only columns A, C, V, W are kept.
    - SoloGuilds: only columns A, S, V are kept.
    """
    os.makedirs(cache_dir, exist_ok=True)

    alliances_csv_url = "https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=1120510750"
    soloGuilds_csv_url = "https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=768688698"

    urls = {
        "Alliances": alliances_csv_url,
        "SoloGuilds": soloGuilds_csv_url
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


def update_world_ids(alliances_df: pd.DataFrame, solo_guilds_df: pd.DataFrame, guild_world_ids: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Updates the world IDs in the alliances and solo guilds dataframes
    based on the latest world ID mapping from the Guild Wars 2 API.

    Args:
        alliances_df (pd.DataFrame): DataFrame containing alliances data.
        solo_guilds_df (pd.DataFrame): DataFrame containing solo guilds data.
        guild_world_ids (dict): Dictionary containing the latest world ID mapping.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Updated dataframes for alliances and solo guilds.
    """
    for index, row in alliances_df.iterrows():
        alliance_id = row['Alliance Guild IDs:'].upper()
        current_world_id = row['World ID']
        new_world_id = guild_world_ids.get(alliance_id, "Unknown")

        if new_world_id != current_world_id:
            alliances_df.at[index, 'World ID'] = new_world_id

    for index, row in solo_guilds_df.iterrows():
        guild_id = row['Guild API ID'].upper()
        current_world_id = row['World']
        new_world_id = guild_world_ids.get(guild_id, "Unknown")

        if new_world_id != current_world_id:
            solo_guilds_df.at[index, 'World'] = new_world_id

    return alliances_df, solo_guilds_df


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
    part = 1  # track part numbers
    for block in alliance_blocks:
        block_len = len(block) + 2
        if current_length + block_len > MAX_FIELD_CHARS and current_alliances:
            # add previous chunk
            title = world_name if part == 1 else f"{world_name} (part-{part})"
            embeds.append(make_embed(title, "\n\n".join(current_alliances)))
            part += 1
            current_alliances, current_length = [], 0
        current_alliances.append(block)
        current_length += block_len

    if current_alliances:
        title = world_name if part == 1 else f"{world_name} (part-{part})"
        embeds.append(make_embed(title, "\n\n".join(current_alliances)))

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



def delete_previous_discord_msgs_for_world_links(cache_file: str) -> None:
    """Retrieve cached links and send DELETE requests for each."""
    world_links = load_data_file(cache_file)
    for world_name, link in world_links.items():
        try:
            resp = requests.delete(link, timeout=10)
            if resp.status_code in (200, 204):
                print(f"✅ Deleted link for {world_name}: {link}")
            else:
                print(f"⚠️ Failed to delete {world_name}: {resp.status_code} {resp.text}")
        except requests.RequestException as e:
            print(f"❌ Error deleting {world_name}: {e}")


def main():
    # Read config
    config_ini = configparser.ConfigParser()
    config_ini.read("config.ini")

    WEBHOOK_URL = config_ini["Settings"]["WEBHOOK_URL"]
    GUILD_ID = config_ini["Settings"]["GUILD_ID"]

    parser = argparse.ArgumentParser(
        description="Process WvW guild data and generate Discord embeds.\n\n "
        "The --remote and --local flags are mutually exclusive.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Mutually exclusive: remote OR local
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--remote", action="store_true", help="Gather Alliance data and Guild from google sheets")
    group.add_argument("--local", action="store_true", help="Gather Alliance data and Guild from local csv files")

    args = parser.parse_args()

    world_links = {}

    
    if not any(vars(args).values()):
        parser.print_help(sys.stderr)
        sys.exit(1)

    if args.remote:
        #pull data from google sheets
        alliances, solo_guilds = fetch_guild_data()
    elif args.local:
        #read data from local files
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
        embeds = build_guild_embeds(world_name, filtered_alliances, filtered_solo_guilds)
        link = post_embeds_and_get_links(WEBHOOK_URL, GUILD_ID, world_name, embeds)
        world_links[world_name] = link
        time.sleep(0.5)

    # Post the summary embed
    summary = build_summary_embed(world_links)
    resp = requests.post(WEBHOOK_URL, json={"embeds": [summary]})
    data = resp.json()
    message_id = data["id"]
    channel_id = data["channel_id"]
    summary_url = f"https://discord.com/channels/{GUILD_ID}/{channel_id}/{message_id}"
    world_links["summary"] = summary_url



if __name__ == "__main__":
    main()