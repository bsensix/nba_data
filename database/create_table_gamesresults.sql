CREATE TABLE "NBA"."GAMES_RESULTS"
(
    "Team_ID" bigint,
    "Game_ID" text,
    "GAME_DATE" timestamp without time zone,
    "MATCHUP" text,
    "WL" text,
    "W" bigint,
    "L" bigint,
    "W_PCT" double precision,
    "MIN" bigint,
    "FGM" bigint,
    "FGA" bigint,
    "FG_PCT" double precision,
    "FG3M" bigint,
    "FG3A" bigint,
    "FG3_PCT" double precision,
    "FTM" bigint,
    "FTA" bigint,
    "FT_PCT" double precision,
    "OREB" bigint,
    "DREB" bigint,
    "REB" bigint,
    "AST" bigint,
    "STL" bigint,
    "BLK" bigint,
    "TOV" bigint,
    "PF" bigint,
    "PTS" bigint,
    "TEAM_1" text,
    "TEAM_2" text,
    CONSTRAINT fk_game_id FOREIGN KEY ("Game_ID")
        REFERENCES "NBA"."GAMES" ("Game_ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
    );