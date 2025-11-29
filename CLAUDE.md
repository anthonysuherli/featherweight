# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

NBA DFS ML system for player projection and lineup optimization.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Scrape NBA game logs
python -m src.data.nba_scraper --season 2024-25 --output data/raw

# Run as module
python -c "from src.data import scrape_season; scrape_season('2024-25')"
```

## Architecture

```
src/
└── data/
    ├── nba_scraper.py   # NBA Stats API scraper (nba_api)
    └── salary_loader.py # DraftKings/FanDuel CSV parsers

data/raw/               # Raw data storage (parquet/csv)
```

### nba_scraper.py
- `NBAStatsScraper` class wraps nba_api endpoints with retry logic and rate limiting
- `get_league_game_logs()` fetches all player game logs for a season in bulk
- `_add_fantasy_points()` calculates DraftKings scoring (PTS, REB, AST, STL, BLK, TOV + DD/TD bonuses)

### salary_loader.py
- `load_draftkings()` / `load_fanduel()` parse platform-specific CSV exports
- `load_salary_file()` auto-detects platform from column headers
- `normalize_player_name()` standardizes names for cross-source matching
