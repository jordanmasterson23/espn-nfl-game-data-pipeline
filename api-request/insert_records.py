import psycopg2
from api_request import fetch_data

def connect_to_db():
    print("Connecting to PostgreSQL db...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise

def create_tables(conn):
    print("Creating table if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS nfl_game_data;
            CREATE TABLE IF NOT EXISTS nfl_game_data.teams (
                team_id            INTEGER PRIMARY KEY,
                uid                TEXT,
                location           TEXT,
                name               TEXT,
                abbreviation       TEXT,
                display_name       TEXT,
                short_display_name TEXT,
                color              TEXT,
                alternate_color    TEXT,
                logo_url           TEXT,
                is_active           BOOLEAN
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.venues (
                venue_id     INTEGER PRIMARY KEY,
                name         TEXT,
                city         TEXT,
                state        TEXT,
                indoor       BOOLEAN,
                capacity     INTEGER
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.athletes (
                athlete_id    INTEGER PRIMARY KEY,
                full_name     TEXT,
                display_name  TEXT,
                short_name    TEXT,
                jersey        TEXT,
                position      TEXT,
                headshot_url  TEXT,
                active        BOOLEAN
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.games (
                game_id          BIGINT PRIMARY KEY,
                uid              TEXT,
                game_date        TIMESTAMP,
                season_year      INTEGER,
                season_type      INTEGER,
                name             TEXT,
                short_name       TEXT,
                status           TEXT,
                status_detail    TEXT,
                period           INTEGER,
                venue_id         INTEGER REFERENCES nfl_game_data.venues(venue_id),
                attendance       INTEGER,
                neutral_site     BOOLEAN
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.game_teams (
                game_id      BIGINT REFERENCES nfl_game_data.games(game_id),
                team_id      INTEGER REFERENCES nfl_game_data.teams(team_id),
                home_away    TEXT CHECK (home_away IN ('home', 'away')),
                score        INTEGER,
                record_type  TEXT,
                record_summary TEXT,

                PRIMARY KEY (game_id, team_id)
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.game_leaders (
                game_id        BIGINT REFERENCES nfl_game_data.games(game_id),
                team_id        INTEGER REFERENCES nfl_game_data.teams(team_id),
                athlete_id     INTEGER REFERENCES nfl_game_data.athletes(athlete_id),
                category       TEXT, -- passing, rushing, receiving
                stat_value     NUMERIC,
                display_value  TEXT,

                PRIMARY KEY (game_id, athlete_id, category)
            );
            CREATE TABLE IF NOT EXISTS nfl_game_data.game_odds (
                game_id        BIGINT REFERENCES nfl_game_data.games(game_id),
                provider       TEXT,
                spread         NUMERIC,
                over_under     NUMERIC,
                details        TEXT,
                favorite_team_id INTEGER REFERENCES nfl_game_data.teams(team_id),

                PRIMARY KEY (game_id, provider)
            );

            CREATE INDEX IF NOT EXISTS idx_games_date ON nfl_game_data.games(game_date);
            CREATE INDEX IF NOT EXISTS idx_game_teams_team ON nfl_game_data.game_teams(team_id);
            CREATE INDEX IF NOT EXISTS idx_game_leaders_team ON nfl_game_data.game_leaders(team_id);
            CREATE INDEX IF NOT EXISTS idx_game_odds_favorite ON nfl_game_data.game_odds(favorite_team_id);
        """)
        conn.commit()
        print("Tables have been created")
    except psycopg2.Error as e:
        print(f"FAILED - An Error occured creating tables: {e}")
        raise


def insert_records(conn, data):
    print("Ingesting NFL Game Data into db...")

    try:
        season_year = data.get("season", {}).get("year")
        season_type = data.get("season", {}).get("type")
        cursor = conn.cursor()
        for event in data["events"]:
            game_id = int(event["id"])
            uid = event.get("uid")
            game_date = event.get("date")
            name = event.get("name")
            short_name = event.get("shortName")

            competition = event["competitions"][0]
            status = competition["status"]["type"]["name"]
            status_detail = competition["status"]["type"]["detail"]
            period = competition["status"].get("period")

            # --------------------
            # VENUE
            # --------------------
            venue = competition.get("venue")
            venue_id = None
            if venue:
                venue_id = int(venue["id"])
                cursor.execute("""
                    INSERT INTO nfl_game_data.venues (
                        venue_id,
                        name, 
                        city, 
                        state, 
                        indoor, 
                        capacity
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (venue_id) DO NOTHING
                """, (
                    venue_id,
                    venue.get("fullName"),
                    venue.get("address", {}).get("city"),
                    venue.get("address", {}).get("state"),
                    venue.get("indoor"),
                    venue.get("capacity")
                ))

            # --------------------
            # GAME
            # --------------------
            cursor.execute("""
                INSERT INTO nfl_game_data.games (
                    game_id, 
                    uid, 
                    game_date,
                    season_year,
                    season_type, 
                    name, 
                    short_name,
                    status, 
                    status_detail, 
                    period, 
                    venue_id,
                    attendance, 
                    neutral_site
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (game_id)
                DO UPDATE SET
                    uid = EXCLUDED.uid,
                    game_date = EXCLUDED.game_date,
                    season_year = EXCLUDED.season_year,
                    season_type = EXCLUDED.season_type,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    status = EXCLUDED.status,
                    status_detail = EXCLUDED.status_detail,
                    period = EXCLUDED.period,
                    venue_id = EXCLUDED.venue_id,
                    attendance = EXCLUDED.attendance,
                    neutral_site = EXCLUDED.neutral_site
            """, (
                game_id, uid, game_date, season_year, 
                season_type, name, short_name,
                status, status_detail, period, venue_id,
                competition.get("attendance"),
                competition.get("neutralSite")
            ))

            # --------------------
            # TEAMS + GAME_TEAMS
            # --------------------
            for competitor in competition["competitors"]:
                team = competitor["team"]
                team_id = int(team["id"])

                cursor.execute("""
                    INSERT INTO nfl_game_data.teams (
                        team_id, 
                        uid,
                        location, 
                        name,
                        abbreviation, 
                        display_name,
                        short_display_name, 
                        color,
                        alternate_color, 
                        logo_url, 
                        is_active
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (team_id)
                    DO UPDATE SET
                        uid = EXCLUDED.uid,
                        location = EXCLUDED.location,
                        name = EXCLUDED.name,
                        abbreviation = EXCLUDED.abbreviation,
                        display_name = EXCLUDED.display_name,
                        short_display_name = EXCLUDED.short_display_name,
                        color = EXCLUDED.color,
                        alternate_color = EXCLUDED.alternate_color,
                        logo_url = EXCLUDED.logo_url,
                        is_active = EXCLUDED.is_active
                """, (
                    team_id,
                    team.get("uid"),
                    team.get("location"),
                    team.get("name"),
                    team.get("abbreviation"),
                    team.get("displayName"),
                    team.get("shortDisplayName"),
                    team.get("color"),
                    team.get("alternateColor"),
                    team.get("logo"),
                    team.get("isActive")
                ))

                cursor.execute("""
                    INSERT INTO nfl_game_data.game_teams (
                        game_id, 
                        team_id, 
                        home_away,
                        score, 
                        record_type, 
                        record_summary
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (game_id, team_id)
                    DO UPDATE SET
                        home_away = EXCLUDED.home_away,
                        score = EXCLUDED.score,
                        record_type = EXCLUDED.record_type,
                        record_summary = EXCLUDED.record_summary
                """, (
                    game_id,
                    team_id,
                    competitor.get("homeAway"),
                    int(competitor.get("score", 0)),
                    competitor.get("records", [{}])[0].get("type"),
                    competitor.get("records", [{}])[0].get("summary")
                ))

                # --------------------
                # LEADERS
                # --------------------
                for leader_group in competitor.get("leaders", []):
                    category = leader_group.get("name")

                    for leader in leader_group.get("leaders", []):
                        athlete = leader["athlete"]
                        athlete_id = int(athlete["id"])

                        cursor.execute("""
                            INSERT INTO nfl_game_data.athletes (
                                athlete_id, 
                                full_name, 
                                display_name,
                                short_name, 
                                jersey, 
                                position,
                                headshot_url, 
                                active
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (athlete_id)
                            DO UPDATE SET
                                full_name = EXCLUDED.full_name,
                                display_name = EXCLUDED.display_name,
                                short_name = EXCLUDED.short_name,
                                jersey = EXCLUDED.jersey,
                                position = EXCLUDED.position,
                                headshot_url = EXCLUDED.headshot_url,
                                active = EXCLUDED.active
                        """, (
                            athlete_id,
                            athlete.get("fullName"),
                            athlete.get("displayName"),
                            athlete.get("shortName"),
                            athlete.get("jersey"),
                            athlete.get("position", {}).get("abbreviation"),
                            athlete.get("headshot"),
                            athlete.get("active")
                        ))

                        cursor.execute("""
                            INSERT INTO nfl_game_data.game_leaders (
                                game_id, 
                                team_id, 
                                athlete_id,
                                category, 
                                stat_value, 
                                display_value
                            ) VALUES (%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (game_id, athlete_id, category)
                            DO UPDATE SET
                                team_id = EXCLUDED.team_id,
                                stat_value = EXCLUDED.stat_value,
                                display_value = EXCLUDED.display_value
                        """, (
                            game_id,
                            team_id,
                            athlete_id,
                            category,
                            leader["stats"][0] if leader.get("stats") else None,
                            leader.get("displayValue")
                        ))

            # --------------------
            # ODDS
            # --------------------
            for odds in competition.get("odds", []):
                cursor.execute("""
                    INSERT INTO nfl_game_data.game_odds (
                        game_id, 
                        provider, 
                        spread,
                        over_under, 
                        details, 
                        favorite_team_id
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (game_id, provider)
                    DO UPDATE SET
                        spread = EXCLUDED.spread,
                        over_under = EXCLUDED.over_under,
                        details = EXCLUDED.details,
                        favorite_team_id = EXCLUDED.favorite_team_id
                """, (
                    game_id,
                    odds["provider"]["name"],
                    odds.get("spread"),
                    odds.get("overUnder"),
                    odds.get("details"),
                    int(odds["favorite"]["id"]) if odds.get("favorite") else None
                ))

        conn.commit()
        print("NFL Game Data has been successfully ingested!")

    except psycopg2.Error as e:
        print(f"FAILED: THere was an error ingesting data: {e}")
        raise

def main():
    try:
        data = fetch_data()
        conn = connect_to_db()
        create_tables(conn)
        insert_records(conn, data)
    except Exception as e:
        print("EXECUTION FAILED: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Connection has been closed.")
