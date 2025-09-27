# GW2-WVW-Teams

A utility for generating **Discord embeds** that display Guild Wars 2 World vs. World (WvW) team compositions.  
The tool organizes data by **World IDs**, **Alliances**, **Alliance Guilds**, and **Solo Guilds**, then posts formatted embeds to a Discord channel via webhook.

---

## Planned Features
- Reads `.csv` exports of the **`Alliances`** and **`SoloGuilds`** worksheets from the official **WvW Guilds Google Sheet**.  
  *(Example CSVs are included in this repository.)*
- Processes data into **per-world summaries** using Pandas.
- Builds **Discord embed messages** and posts them to your configured server.
- Optionally fetches **world assignments** via GW2 API (`fetch_north_american_guilds()`).
- Can detect and update only when alliance/world changes occur.

---

## Setup

### 1. Configure Discord Settings
Edit `config.ini` with your Discord server details:

```ini
[discord]
WEBHOOK_URL = https://discord.com/api/webhooks/your_webhook_id/your_webhook_token
GUILD_ID = 123456789012345678
````

To find your `GUILD_ID`, copy a message link from the target channel.
The format is:

```
https://discord.com/channels/<guild_id>/<channel_id>/<message_id>
```

Validate that the links are still valid for remote resources

```py
ALLIANCES_REMOTE_SHEET_URL
SOLO_GUILDS_REMOTE_SHEET_URL 
GW2_NA_GUILDS_API_URL 
```
---

### 2. Install Dependencies

Requires **Python 3.9+**.

```bash
pip install -r requirements.txt
```

---

### 3. Prepare CSV Data

If you are running with `--local` , export the latest worksheets from the **WvW Guilds Google Sheet**:

* `Alliances` → `WvW Guilds - Alliances.csv`
* `SoloGuilds` → `WvW Guilds - SoloGuilds.csv`

Place both files in the same directory as `wvw-teams.py`.
*(The default export filenames will match the above.)*

---

## Usage

From the project directory, run:

```bash
# Use local CSV exports
python wvw-teams.py --local

# Fetch data from Google Sheets
python wvw-teams.py --remote
```

The script will:

1. Process the alliance/solo guild data.
2. Build per-world embeds.
3. Post them to your Discord channel.

**Example Output:**

![World Embeds](WvW-Teams-Embed.png)
![Summary Embed](wvw-teams-summary.png)

---

## Requirements

* Python 3.9+
* [requests](https://pypi.org/project/requests/)
* [pandas](https://pypi.org/project/pandas/)

Install with:

```bash
pip install -r requirements.txt
```

---

## Roadmap / Planned Improvements

* Automatic Google Sheets integration
* Smarter caching and change detection
* Error handling and retry logic
* Automated cron/scheduled updates
* Weekly reset helpers (e.g., one-up-one-down match data)

---

## Development Notes

The current design follows these steps:

1. **Fetch GW2 API Guild Data**

   * `fetch_north_american_guilds()`

2. **Compare with Cached Data**

   * `detect_world_changes(prev, curr)`
   * `cache_data_file()` / `load_data_file()`

3. **If Changes Detected → Continue**
   Otherwise exit.

4. **Fetch Google Sheet Data**

   * `fetch_guild_data()`

5. **Update World Assignments**

   * `update_world_ids()`

6. **Clean Up Old Messages**

   * `delete_previous_discord_msgs_for_world_links()`

7. **Build Embeds Per World**

   * `build_guild_embeds()`

8. **Post Embeds to Discord**

   * `post_embeds_and_get_links()`

9. **Post Summary Embed with Links**

   * `build_summary_embed()`

10. **Cache New Links/Data**

---

## Contributing

This project is currently a **prototype** and evolving.
Suggestions, issues, and pull requests are very welcome!

---

## License

GPL-3.0 license – see [LICENSE](https://github.com/Drevarr/GW2-WVW-Teams#GPL-3.0-1-ov-file) for details.

```