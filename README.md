# featherweight

NBA DFS ML system for player projection and lineup optimization.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### Scrape NBA game logs

```python
from src.data import NBAStatsScraper, scrape_season

# Full season scrape
df = scrape_season("2024-25")

# Or use the class directly
scraper = NBAStatsScraper()
df = scraper.get_league_game_logs(season="2024-25")
```

CLI:
```bash
python -m src.data.nba_scraper --season 2024-25 --output data/raw
```

### Load DFS salaries

```python
from src.data import load_draftkings, load_fanduel, load_salary_file

# Platform-specific
salaries = load_draftkings("data/raw/DKSalaries.csv")
salaries = load_fanduel("data/raw/FDSalaries.csv")

# Auto-detect platform
salaries = load_salary_file("data/raw/salaries.csv")
```
