"""NBA Stats API scraper for player game logs and team statistics."""

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
from nba_api.stats.endpoints import (
    commonallplayers,
    leaguegamelog,
    playergamelog,
    teamestimatedmetrics,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class NBAStatsScraper:
    """Scraper for NBA Stats API data."""

    def __init__(self, delay: float = 0.6):
        """
        Initialize scraper.

        Args:
            delay: Seconds to wait between API calls.
        """
        self.delay = delay

    def _wait(self) -> None:
        """Wait between API calls."""
        time.sleep(self.delay)

    def _api_call_with_retry(self, endpoint_func, max_retries: int = 3, **kwargs) -> pd.DataFrame:
        """
        Execute API call with retry logic.

        Args:
            endpoint_func: The nba_api endpoint class.
            max_retries: Maximum retry attempts.
            **kwargs: Arguments to pass to the endpoint.

        Returns:
            DataFrame from the API call.
        """
        for attempt in range(max_retries):
            try:
                endpoint = endpoint_func(**kwargs)
                df = endpoint.get_data_frames()[0]
                self._wait()
                return df
            except Exception as e:
                wait_time = (2 ** attempt) * self.delay
                if attempt < max_retries - 1:
                    logger.warning(f"API call failed (attempt {attempt + 1}): {e}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API call failed after {max_retries} attempts: {e}")
                    raise
        return pd.DataFrame()

    def get_league_game_logs(
        self,
        season: str = "2024-25",
        season_type: str = "Regular Season",
        date_from: str = None,
        date_to: str = None,
    ) -> pd.DataFrame:
        """
        Fetch all player game logs for a season.

        Args:
            season: Season string format "YYYY-YY".
            season_type: "Regular Season" or "Playoffs".
            date_from: Optional start date filter (MM/DD/YYYY).
            date_to: Optional end date filter (MM/DD/YYYY).

        Returns:
            DataFrame with all player game logs and fantasy_points column.
        """
        logger.info(f"Fetching league game logs for {season} {season_type}")
        try:
            df = self._api_call_with_retry(
                leaguegamelog.LeagueGameLog,
                season=season,
                player_or_team_abbreviation="P",
                season_type_all_star=season_type,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
            )
            logger.info(f"Retrieved {len(df)} game log entries")
            return self._add_fantasy_points(df)
        except Exception as e:
            logger.error(f"Failed to fetch league game logs: {e}")
            return pd.DataFrame()

    def get_player_game_log(
        self,
        player_id: int,
        season: str = "2024-25",
        season_type: str = "Regular Season",
    ) -> pd.DataFrame:
        """
        Fetch game log for a single player.

        Args:
            player_id: NBA player ID.
            season: Season string format "YYYY-YY".
            season_type: "Regular Season" or "Playoffs".

        Returns:
            DataFrame with player's game logs.
        """
        try:
            df = self._api_call_with_retry(
                playergamelog.PlayerGameLog,
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
            )
            return self._add_fantasy_points(df)
        except Exception as e:
            logger.warning(f"Failed to fetch game log for player {player_id}: {e}")
            return pd.DataFrame()

    def get_all_players(
        self,
        season: str = "2024-25",
        active_only: bool = True,
    ) -> pd.DataFrame:
        """
        Get list of all players for a season.

        Args:
            season: Season string format "YYYY-YY".
            active_only: If True, only return current season players.

        Returns:
            DataFrame with player info (PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ID, TEAM_ABBREVIATION).
        """
        logger.info(f"Fetching all players for {season}")
        try:
            df = self._api_call_with_retry(
                commonallplayers.CommonAllPlayers,
                is_only_current_season=1 if active_only else 0,
                season=season,
            )
            logger.info(f"Retrieved {len(df)} players")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch players: {e}")
            return pd.DataFrame()

    def get_team_stats(self, season: str = "2024-25") -> pd.DataFrame:
        """
        Get team-level statistics for opponent adjustments.

        Args:
            season: Season string format "YYYY-YY".

        Returns:
            DataFrame with team offensive/defensive ratings.
        """
        logger.info(f"Fetching team stats for {season}")
        try:
            df = self._api_call_with_retry(
                teamestimatedmetrics.TeamEstimatedMetrics,
                season=season,
            )
            logger.info(f"Retrieved stats for {len(df)} teams")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch team stats: {e}")
            return pd.DataFrame()

    def _add_fantasy_points(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate DraftKings fantasy points.

        Args:
            df: DataFrame with game log stats.

        Returns:
            DataFrame with fantasy_points column added.
        """
        if df.empty:
            return df

        df = df.copy()

        # Handle both uppercase and lowercase column names
        def get_col(name: str) -> pd.Series:
            if name in df.columns:
                return df[name].fillna(0)
            if name.lower() in df.columns:
                return df[name.lower()].fillna(0)
            return pd.Series(0, index=df.index)

        pts = get_col("PTS")
        fg3m = get_col("FG3M")
        reb = get_col("REB")
        ast = get_col("AST")
        stl = get_col("STL")
        blk = get_col("BLK")
        tov = get_col("TOV")

        # Base fantasy points
        fantasy_points = (
            pts * 1.0
            + fg3m * 0.5
            + reb * 1.25
            + ast * 1.5
            + stl * 2.0
            + blk * 2.0
            - tov * 0.5
        )

        # Double-double and triple-double bonuses
        double_digit_cats = (
            (pts >= 10).astype(int)
            + (reb >= 10).astype(int)
            + (ast >= 10).astype(int)
            + (stl >= 10).astype(int)
            + (blk >= 10).astype(int)
        )

        fantasy_points += (double_digit_cats >= 2).astype(float) * 1.5  # Double-double
        fantasy_points += (double_digit_cats >= 3).astype(float) * 1.5  # Triple-double bonus

        df["fantasy_points"] = fantasy_points
        return df


def scrape_season(
    season: str = "2024-25",
    output_path: str = None,
) -> pd.DataFrame:
    """
    Convenience function to scrape a full season and save to parquet.

    Args:
        season: Season string format "YYYY-YY".
        output_path: Output file path. Defaults to data/raw/game_logs_{season}.parquet.

    Returns:
        DataFrame with all game logs.
    """
    if output_path is None:
        season_str = season.replace("-", "_")
        output_path = f"data/raw/game_logs_{season_str}.parquet"

    scraper = NBAStatsScraper()
    df = scraper.get_league_game_logs(season=season)

    if not df.empty:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} rows to {output_path}")

    return df


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Scrape NBA player game logs")
    parser.add_argument("--season", default="2024-25", help="Season to scrape (default: 2024-25)")
    parser.add_argument("--output", default="data/raw", help="Output directory (default: data/raw)")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format")
    args = parser.parse_args()

    scraper = NBAStatsScraper()
    df = scraper.get_league_game_logs(season=args.season)

    if df.empty:
        logger.error("No data retrieved")
        return

    season_str = args.season.replace("-", "_")
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.format == "parquet":
        output_path = output_dir / f"game_logs_{season_str}.parquet"
        df.to_parquet(output_path, index=False)
    else:
        output_path = output_dir / f"game_logs_{season_str}.csv"
        df.to_csv(output_path, index=False)

    logger.info(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
