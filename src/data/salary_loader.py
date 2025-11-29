"""DraftKings and FanDuel salary CSV loaders."""

import re

import pandas as pd


def normalize_player_name(name: str) -> str:
    """
    Standardize player names for matching across sources.

    Args:
        name: Raw player name.

    Returns:
        Normalized lowercase name without periods or suffixes.
    """
    if not isinstance(name, str):
        return ""

    name = name.lower()
    name = name.replace(".", "")
    name = re.sub(r"\s+(jr|sr|iii|ii|iv)\.?$", "", name, flags=re.IGNORECASE)
    name = " ".join(name.split())
    return name


def _parse_matchup(matchup: str, player_team: str) -> tuple[str, bool]:
    """
    Parse matchup string to extract opponent and home/away status.

    Args:
        matchup: Matchup string (e.g., "PHX@LAL 10:30PM ET").
        player_team: Player's team abbreviation.

    Returns:
        Tuple of (opponent, is_home).
    """
    if not isinstance(matchup, str) or "@" not in matchup:
        return "", False

    matchup_part = matchup.split()[0]
    teams = matchup_part.split("@")

    if len(teams) != 2:
        return "", False

    away_team, home_team = teams[0], teams[1]

    if player_team.upper() == home_team.upper():
        return away_team.upper(), True
    else:
        return home_team.upper(), False


def load_draftkings(filepath: str) -> pd.DataFrame:
    """
    Load DraftKings salary CSV into standardized format.

    Args:
        filepath: Path to DraftKings CSV export.

    Returns:
        DataFrame with standardized columns.
    """
    df = pd.read_csv(filepath)

    result = pd.DataFrame()
    result["name"] = df["Name"]
    result["position"] = df["Position"].apply(lambda x: x.split("/")[0] if isinstance(x, str) else x)
    result["positions"] = df["Position"].apply(lambda x: x.split("/") if isinstance(x, str) else [x])
    result["salary"] = df["Salary"].astype(int)
    result["avg_fpts"] = df["AvgPointsPerGame"].astype(float)
    result["team"] = df["TeamAbbrev"]

    parsed = df.apply(
        lambda row: _parse_matchup(row.get("Game Info", ""), row.get("TeamAbbrev", "")),
        axis=1,
    )
    result["opponent"] = parsed.apply(lambda x: x[0])
    result["is_home"] = parsed.apply(lambda x: x[1])

    return result


def load_fanduel(filepath: str) -> pd.DataFrame:
    """
    Load FanDuel salary CSV into standardized format.

    Args:
        filepath: Path to FanDuel CSV export.

    Returns:
        DataFrame with standardized columns.
    """
    df = pd.read_csv(filepath)

    result = pd.DataFrame()
    result["name"] = df["Nickname"]
    result["position"] = df["Position"].apply(lambda x: x.split("/")[0] if isinstance(x, str) else x)
    result["positions"] = df["Position"].apply(lambda x: x.split("/") if isinstance(x, str) else [x])
    result["salary"] = df["Salary"].astype(int)
    result["avg_fpts"] = df["FPPG"].astype(float)
    result["team"] = df["Team"]
    result["opponent"] = df["Opponent"]

    if "Game" in df.columns:
        result["is_home"] = df.apply(
            lambda row: not str(row.get("Game", "")).startswith(str(row.get("Team", ""))),
            axis=1,
        )
    else:
        result["is_home"] = False

    if "Injury Indicator" in df.columns:
        result["injury_status"] = df["Injury Indicator"]
    if "Injury Details" in df.columns:
        result["injury_details"] = df["Injury Details"]

    return result


def load_salary_file(filepath: str, platform: str = None) -> pd.DataFrame:
    """
    Load salary file with auto-detection of platform.

    Args:
        filepath: Path to salary CSV.
        platform: "draftkings" or "fanduel". Auto-detects if None.

    Returns:
        DataFrame with standardized columns.

    Raises:
        ValueError: If platform cannot be detected.
    """
    if platform:
        platform = platform.lower()
        if platform in ("draftkings", "dk"):
            return load_draftkings(filepath)
        elif platform in ("fanduel", "fd"):
            return load_fanduel(filepath)
        else:
            raise ValueError(f"Unknown platform: {platform}")

    df = pd.read_csv(filepath)

    if "AvgPointsPerGame" in df.columns:
        return load_draftkings(filepath)
    elif "FPPG" in df.columns:
        return load_fanduel(filepath)
    else:
        raise ValueError("Unknown salary file format. Could not auto-detect platform.")
