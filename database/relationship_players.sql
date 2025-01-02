-- Games and Players relationship
ALTER TABLE "NBA"."PLAYER_RESULTS"
ADD CONSTRAINT fk_game_id
FOREIGN KEY ("Game_ID") REFERENCES "NBA"."GAMES"("Game_ID");

-- Players and Player_Results relationship
ALTER TABLE "NBA"."PLAYERS"
ADD PRIMARY KEY ("id");

ALTER TABLE "NBA"."PLAYER_RESULTS"
ADD CONSTRAINT fk_player_id
FOREIGN KEY ("Player_ID") REFERENCES "NBA"."PLAYERS"("id");