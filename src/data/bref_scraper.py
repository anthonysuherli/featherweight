"""Basketball Reference scraper for player stats and game logs."""

import logging
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.basketball-reference.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class BRefScraper:
    """Scraper for Basketball Reference data."""

    def __init__(self, delay: float = 3.1):
        """
        Initialize scraper.

        Args:
            delay: Seconds between requests. Basketball Reference limits to 20 req/min.
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _wait(self) -> None:
        """Wait between requests to respect rate limits."""
        time.sleep(self.delay)

    def _fetch(self, url: str, max_retries: int = 3) -> requests.Response | None:
        """
        Fetch URL with retry logic.

        Args:
            url: URL to fetch.
            max_retries: Maximum retry attempts.

        Returns:
            Response object or None on failure.
        """
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                self._wait()
                return response
            except requests.RequestException as e:
                wait_time = (2 ** attempt) * self.delay
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}): {e}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
        return None

    def _parse_tables(self, html: str, table_id: str = None) -> list[pd.DataFrame]:
        """
        Parse HTML tables including those hidden in comments.

        Args:
            html: Raw HTML content.
            table_id: Optional table ID to filter.

        Returns:
            List of DataFrames found.
        """
        soup = BeautifulSoup(html, "html.parser")
        tables = []

        # Standard tables
        try:
            if table_id:
                standard = pd.read_html(html, attrs={"id": table_id})
            else:
                standard = pd.read_html(html)
            tables.extend(standard)
        except ValueError:
            pass

        # Tables hidden in comments
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if "table" in str(comment):
                try:
                    if table_id:
                        commented = pd.read_html(str(comment), attrs={"id": table_id})
                    else:
                        commented = pd.read_html(str(comment))
                    tables.extend(commented)
                except ValueError:
                    continue

        return tables

    def _player_url_slug(self, name: str) -> str:
        """
        Convert player name to Basketball Reference URL slug.

        Args:
            name: Player full name (e.g., "LeBron James").

        Returns:
            URL slug (e.g., "jamesle01").
        """
        parts = name.lower().split()
        if len(parts) < 2:
            return ""
        first, last = parts[0], parts[-1]
        # Standard format: first 5 of last + first 2 of first + 01
        slug = f"{last[:5]}{first[:2]}01"
        return slug

    def get_player_game_logs(
        self,
        name: str,
        season: int,
        playoffs: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch game logs for a player in a specific season.

        Args:
            name: Player full name.
            season: Season ending year (e.g., 2025 for 2024-25 season).
            playoffs: If True, fetch playoff games.

        Returns:
            DataFrame with game log stats.
        """
        slug = self._player_url_slug(name)
        if not slug:
            logger.error(f"Could not generate URL slug for {name}")
            return pd.DataFrame()

        letter = slug[0]
        game_type = "playoffs" if playoffs else "gamelog"
        url = f"{BASE_URL}/players/{letter}/{slug}/{game_type}/{season}"

        logger.info(f"Fetching game logs for {name} ({season})")
        response = self._fetch(url)
        if not response:
            return pd.DataFrame()

        tables = self._parse_tables(response.text, table_id="pgl_basic")
        if not tables:
            logger.warning(f"No game log table found for {name}")
            return pd.DataFrame()

        df = tables[0]
        df = self._clean_game_log(df)
        df["player_name"] = name
        df["season"] = season
        return self._add_fantasy_points(df)

    def get_season_stats(
        self,
        season: int,
        stat_type: str = "per_game",
        playoffs: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch all player stats for a season.

        Args:
            season: Season ending year (e.g., 2025 for 2024-25 season).
            stat_type: One of "per_game", "totals", "per_minute", "per_poss", "advanced".
            playoffs: If True, fetch playoff stats.

        Returns:
            DataFrame with all player stats.
        """
        stat_map = {
            "per_game": "per_game",
            "totals": "totals",
            "per_minute": "per_minute",
            "per_poss": "per_poss",
            "advanced": "advanced",
        }
        if stat_type not in stat_map:
            logger.error(f"Invalid stat_type: {stat_type}")
            return pd.DataFrame()

        base = "playoffs" if playoffs else "leagues"
        url = f"{BASE_URL}/{base}/NBA_{season}_{stat_map[stat_type]}.html"

        logger.info(f"Fetching {stat_type} stats for {season}")
        response = self._fetch(url)
        if not response:
            return pd.DataFrame()

        table_ids = {
            "per_game": "per_game_stats",
            "totals": "totals_stats",
            "per_minute": "per_minute_stats",
            "per_poss": "per_poss_stats",
            "advanced": "advanced_stats",
        }

        tables = self._parse_tables(response.text, table_id=table_ids[stat_type])
        if not tables:
            logger.warning(f"No {stat_type} table found for {season}")
            return pd.DataFrame()

        df = tables[0]
        df = self._clean_season_stats(df)
        df["season"] = season
        return df

    def get_team_ratings(self, season: int) -> pd.DataFrame:
        """
        Fetch team offensive/defensive ratings.

        Args:
            season: Season ending year.

        Returns:
            DataFrame with team ratings.
        """
        url = f"{BASE_URL}/leagues/NBA_{season}_ratings.html"

        logger.info(f"Fetching team ratings for {season}")
        response = self._fetch(url)
        if not response:
            return pd.DataFrame()

        tables = self._parse_tables(response.text, table_id="ratings")
        if not tables:
            return pd.DataFrame()

        df = tables[0]
        df["season"] = season
        return df

    def _clean_game_log(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean game log DataFrame."""
        if df.empty:
            return df

        df = df.copy()

        # Remove header rows that appear mid-table
        if "Rk" in df.columns:
            df = df[df["Rk"] != "Rk"]

        # Remove rows with all NaN
        df = df.dropna(how="all")

        # Convert numeric columns
        numeric_cols = ["PTS", "TRB", "AST", "STL", "BLK", "TOV", "FG", "FGA", "3P", "3PA", "FT", "FTA", "MP"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _clean_season_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean season stats DataFrame."""
        if df.empty:
            return df

        df = df.copy()

        # Remove header rows
        if "Rk" in df.columns:
            df = df[df["Rk"] != "Rk"]

        # Remove rows with all NaN
        df = df.dropna(how="all")

        # Standardize column names
        col_map = {
            "Player": "player_name",
            "Pos": "position",
            "Tm": "team",
            "G": "games",
            "GS": "games_started",
        }
        df = df.rename(columns=col_map)

        return df

    def _add_fantasy_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate DraftKings fantasy points.

        Args:
            df: DataFrame with game stats.

        Returns:
            DataFrame with fantasy_points column.
        """
        if df.empty:
            return df

        df = df.copy()

        def get_col(name: str) -> pd.Series:
            for col in [name, name.upper(), name.lower()]:
                if col in df.columns:
                    return pd.to_numeric(df[col], errors="coerce").fillna(0)
            return pd.Series(0, index=df.index)

        pts = get_col("PTS")
        fg3m = get_col("3P")
        reb = get_col("TRB")
        ast = get_col("AST")
        stl = get_col("STL")
        blk = get_col("BLK")
        tov = get_col("TOV")

        fantasy_points = (
            pts * 1.0
            + fg3m * 0.5
            + reb * 1.25
            + ast * 1.5
            + stl * 2.0
            + blk * 2.0
            - tov * 0.5
        )

        # Double-double / triple-double bonuses
        double_digit_cats = (
            (pts >= 10).astype(int)
            + (reb >= 10).astype(int)
            + (ast >= 10).astype(int)
            + (stl >= 10).astype(int)
            + (blk >= 10).astype(int)
        )

        fantasy_points += (double_digit_cats >= 2).astype(float) * 1.5
        fantasy_points += (double_digit_cats >= 3).astype(float) * 1.5

        df["fantasy_points"] = fantasy_points
        return df


def scrape_season_stats(
    season: int = 2025,
    stat_type: str = "per_game",
    output_path: str = None,
) -> pd.DataFrame:
    """
    Convenience function to scrape season stats.

    Args:
        season: Season ending year.
        stat_type: Stat type to fetch.
        output_path: Output file path.

    Returns:
        DataFrame with stats.
    """
    if output_path is None:
        output_path = f"data/raw/bref_stats_{season}_{stat_type}.parquet"

    scraper = BRefScraper()
    df = scraper.get_season_stats(season=season, stat_type=stat_type)

    if not df.empty:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} rows to {output_path}")

    return df


def scrape_player_logs(
    name: str,
    season: int = 2025,
    output_path: str = None,
) -> pd.DataFrame:
    """
    Convenience function to scrape player game logs.

    Args:
        name: Player full name.
        season: Season ending year.
        output_path: Output file path.

    Returns:
        DataFrame with game logs.
    """
    scraper = BRefScraper()
    df = scraper.get_player_game_logs(name=name, season=season)

    if not df.empty and output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} rows to {output_path}")

    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Basketball Reference data")
    parser.add_argument("--season", type=int, default=2025, help="Season ending year")
    parser.add_argument("--stat-type", default="per_game", choices=["per_game", "totals", "per_minute", "per_poss", "advanced"])
    parser.add_argument("--output", default="data/raw", help="Output directory")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    args = parser.parse_args()

    scraper = BRefScraper()
    df = scraper.get_season_stats(season=args.season, stat_type=args.stat_type)

    if df.empty:
        logger.error("No data retrieved")
    else:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"bref_{args.stat_type}_{args.season}"
        if args.format == "parquet":
            output_path = output_dir / f"{filename}.parquet"
            df.to_parquet(output_path, index=False)
        else:
            output_path = output_dir / f"{filename}.csv"
            df.to_csv(output_path, index=False)

        logger.info(f"Saved {len(df)} rows to {output_path}")
