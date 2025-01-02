CREATE TABLE "NBA"."PLAYERS"
(
    id bigint NOT NULL,
    full_name text,
    first_name text,
    last_name text,
    is_active boolean,
    CONSTRAINT "PLAYERS_pkey" PRIMARY KEY (id)
);