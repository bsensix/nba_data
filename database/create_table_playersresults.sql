CREATE TABLE "NBA"."PLAYER_RESULTS"
(
    "SEASON_ID" text,
    "Player_ID" bigint,
    "Game_ID" text,
    "GAME_DATE" text,
    "MATCHUP" text,
    "WL" text,
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
    "PLUS_MINUS" bigint,
    "VIDEO_AVAILABLE" bigint,
    CONSTRAINT fk_game_id FOREIGN KEY ("Game_ID")
        REFERENCES "NBA"."GAMES" ("Game_ID") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_player_id FOREIGN KEY ("Player_ID")
        REFERENCES "NBA"."PLAYERS" (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);