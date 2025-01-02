CREATE TABLE "NBA"."GAMES"
(
    "Game_ID" text NOT NULL,
    "GAME_DATE" timestamp without time zone,
    "TEAM_1" text,
    "TEAM_2" text,
    CONSTRAINT "GAMES_pkey" PRIMARY KEY ("Game_ID")
);