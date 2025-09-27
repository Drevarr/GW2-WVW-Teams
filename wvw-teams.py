import argparse
import configparser
from datetime import datetime, timezone
import os
import sys
import time
from typing import List, Tuple, Dict, Union
import json
from urllib.parse import urlparse
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
    

def fetch_north_american_guilds() -> pd.DataFrame:
    """
    Fetch the listing of North American Guilds from the Guild Wars 2 API.

    The GW2API is queried for the list of North American Guilds. The response is
    parsed as JSON and returned as a pandas DataFrame.

    Parameters:
        None

    Returns:
        pd.DataFrame: A DataFrame with columns ['guild_id', 'world_id'].
    """
    url = "https://api.guildwars2.com/v2/wvw/guilds/na"
    try:
        # Send a GET request to the GW2API with a timeout
        response = requests.get(url, timeout=(3.0, 5))
        response.raise_for_status()

        # Parse the JSON into a dict
        data = response.json()

        # Convert to DataFrame
        guilds_df = pd.DataFrame(list(data.items()), columns=["guild_id", "world_id"])
        return guilds_df

    except requests.exceptions.RequestException as error:
        print(f"Error: {error}")
        return pd.DataFrame(columns=["guild_id", "world_id"])
    

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


def update_world_ids(
    alliances_df: pd.DataFrame,
    solo_guilds_df: pd.DataFrame,
    guild_world_ids: pd.DataFrame,
    world_id_map: Dict[int, str],
    alliance_id_col: str = 'Alliance Guild IDs:',
    alliance_name_col: str = 'Alliance:',
    alliance_world_col: str = 'World ID',
    solo_id_col: str = 'Guild API ID',
    solo_world_col: str = 'World'
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str]]:
    """
    Updates world IDs in alliances and solo guilds DataFrames based on the latest world ID mapping.

    Args:
        alliances_df (pd.DataFrame): DataFrame with alliances data.
        solo_guilds_df (pd.DataFrame): DataFrame with solo guilds data.
        guild_world_ids (pd.DataFrame): DataFrame with guild_id and world_id columns.
        world_id_map (Dict[int, str]): Mapping of world IDs to world names.
        alliance_id_col (str): Column name for alliance guild IDs.
        alliance_name_col (str): Column name for alliance names.
        alliance_world_col (str): Column name for alliance world IDs.
        solo_id_col (str): Column name for solo guild IDs.
        solo_world_col (str): Column name for solo guild world IDs.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str]]: Updated DataFrames and
        dictionaries tracking changed and unchanged world assignments.

    Raises:
        ValueError: If required columns are missing or inputs are invalid.
    """
    # Validate inputs
    required_cols = {
        'alliances_df': [alliance_id_col, alliance_name_col, alliance_world_col],
        'solo_guilds_df': [solo_id_col, solo_world_col],
        'guild_world_ids': ['guild_id', 'world_id']
    }
    for df_name, cols in required_cols.items():
        df = locals()[df_name]
        if not all(col in df.columns for col in cols):
            raise ValueError(f"{df_name} missing required columns: {cols}")

    changed = {}
    unchanged = {}

    # Normalize guild IDs to uppercase
    alliances_df = alliances_df.copy()
    solo_guilds_df = solo_guilds_df.copy()
    guild_world_ids = guild_world_ids.copy()
    guild_world_ids['guild_id'] = guild_world_ids['guild_id'].str.upper()
    alliances_df[alliance_id_col] = alliances_df[alliance_id_col].str.upper()
    solo_guilds_df[solo_id_col] = solo_guilds_df[solo_id_col].str.upper()

    # Update alliances_df
    alliances_df = alliances_df.merge(
        guild_world_ids[['guild_id', 'world_id']],
        how='left',
        left_on=alliance_id_col,
        right_on='guild_id'
    )
    alliances_df['new_world_name'] = alliances_df['world_id'].map(world_id_map).fillna("")
    
    # Track changes for alliances
    for _, row in alliances_df.iterrows():
        alliance_name = row[alliance_name_col]
        current_world = row[alliance_world_col]
        new_world = row['new_world_name']
        if new_world and new_world != current_world:
            changed[alliance_name] = f"Moved from {current_world} to {new_world}"
            alliances_df.loc[alliances_df[alliance_name_col] == alliance_name, alliance_world_col] = new_world
        else:
            unchanged[alliance_name] = f"Remained on {current_world or new_world}"

    # Update solo_guilds_df
    solo_guilds_df = solo_guilds_df.merge(
        guild_world_ids[['guild_id', 'world_id']],
        how='left',
        left_on=solo_id_col,
        right_on='guild_id'
    )
    solo_guilds_df['new_world_name'] = solo_guilds_df['world_id'].map(world_id_map).fillna("")
    
    # Track changes for solo guilds
    for _, row in solo_guilds_df.iterrows():
        guild_id = row[solo_id_col]
        current_world = row[solo_world_col]
        new_world = row['new_world_name']
        if new_world and new_world != current_world:
            changed[guild_id] = f"Moved from {current_world} to {new_world}"
            solo_guilds_df.loc[solo_guilds_df[solo_id_col] == guild_id, solo_world_col] = new_world
        else:
            unchanged[guild_id] = f"Remained on {current_world or new_world}"

    # Clean up temporary columns
    alliances_df = alliances_df.drop(columns=['guild_id', 'world_id', 'new_world_name'], errors='ignore')
    solo_guilds_df = solo_guilds_df.drop(columns=['guild_id', 'world_id', 'new_world_name'], errors='ignore')

    return alliances_df, solo_guilds_df, changed, unchanged


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
    

def post_embeds_and_get_links(webhook_url: str, guild_id: str, embeds: list) -> list[str]:
    """
    Post embeds for one world and return a list of message links.
    The first item in the list will always be the first message link.
    """
    if not webhook_url.endswith("?wait=true"):
        webhook_url = webhook_url + "?wait=true"

    links = []
    for embed in embeds:
        resp = requests.post(webhook_url, json={"embeds": [embed]})
        resp.raise_for_status()  # good practice
        data = resp.json()
        message_id = data["id"]
        channel_id = data["channel_id"]
        jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
        links.append(jump_url)

    return links


def build_summary_embed(world_links: dict) -> dict:
    """Build a summary embed linking to the *first* message of each world."""
    lines = [
        f"[{world}]({links[0]})"
        for world, links in world_links.items()
        if links
    ]
    return {
        "title": "WvW Guild Lists Summary",
        "description": "\n".join(lines),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def post_summary_embed(webhook_url: str, guild_id: str, summary: dict, msg_links: dict) -> dict:
    """
    Post a summary embed to Discord and update the message links dictionary.

    Args:
        webhook_url (str): URL of the Discord webhook.
        guild_id (str): ID of the Discord guild.
        summary (dict): Summary embed data.
        msg_links (dict): Dictionary of world names to message links.

    Returns:
        dict: Updated message links dictionary.
    """
    # Append "?wait=true" to the webhook URL if not already present
    if not webhook_url.endswith("?wait=true"):
        webhook_url = webhook_url + "?wait=true"    

    # Post the summary embed to Discord
    resp = requests.post(webhook_url, json={"embeds": [summary]})
    data = resp.json()
    message_id = data["id"]
    channel_id = data["channel_id"]

    # Construct the summary message link
    summary_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    # Update the message links dictionary
    msg_links["Summary"] = summary_url
    return msg_links


def delete_previous_discord_msgs_for_world_links(WEBHOOK_URL: str, cache_file: str) -> None:
    """Retrieve cached links and send DELETE requests for each."""
    if not os.path.exists(cache_file):
        print(f"⚠️ Prior Message link file {cache_file} not found. Skipping...")
        return

    world_links = load_data_file(cache_file)

    for world_name, links in world_links.items():
        if not isinstance(links, list):
            links = [links]  # backward compatibility if cache still has single strings

        for link in links:
            try:
                parts = urlparse(link).path.strip("/").split("/")
                if len(parts) < 4 or parts[0] != "channels":
                    print(f"⚠️ Invalid UI link format for {world_name}: {link}")
                    continue

                message_id = parts[-1]  # last element is the message_id
                delete_url = f"{WEBHOOK_URL}/messages/{message_id}"

                resp = requests.delete(delete_url, timeout=10)
                time.sleep(0.5)

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

    #load previous world data:
    cached_Alliances = load_data_file("cached_Alliances")
    cached_Solo_Guilds = load_data_file("cached_Solo_Guilds")

    #compare old to new data
    if cached_Alliances is not None and cached_Solo_Guilds is not None:
        pass
    else:
        #print("⚠️ No cached data found. Skipping...")
        
    #delete discord messages for each world if previous file exists
    delete_previous_discord_msgs_for_world_links(WEBHOOK_URL, "previous_discord_messages.json")

    #build_discord_embeds
    for world_name in world_list:
        filtered_alliances = sorted_alliances.loc[sorted_alliances['World ID'] == world_name]
        filtered_solo_guilds = sorted_solo_guilds.loc[sorted_solo_guilds['World'] == world_name]
        embeds = build_guild_embeds(world_name, filtered_alliances, filtered_solo_guilds)
        link = post_embeds_and_get_links(WEBHOOK_URL, GUILD_ID, embeds)
        world_links[world_name] = link
        time.sleep(0.5)

    # Post the summary embed
    summary = build_summary_embed(world_links)
    previous_discord_messages = post_summary_embed(WEBHOOK_URL, GUILD_ID, summary, world_links)
    #save discord messages to cache file for reference on future deletions
    cache_data_file(previous_discord_messages, "previous_discord_messages.json")


if __name__ == "__main__":
    main()