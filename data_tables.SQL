CREATE TABLE bars (
    bar_id   INTEGER NOT NULL PRIMARY KEY,
    location TEXT    NOT NULL
);

CREATE TABLE glasses (
    glass_id   INTEGER NOT NULL PRIMARY KEY,
    glass_name TEXT    NOT NULL
);

CREATE TABLE drinks (
    drink_id       INTEGER NOT NULL PRIMARY KEY,
    drink_name     TEXT    NOT NULL,
    glass_id       INTEGER NOT NULL,
    cocktail_db_id INTEGER NOT NULL
);

CREATE TABLE inventory (
    inventory_id    INTEGER NOT NULL PRIMARY KEY,
    glass_id        INTEGER NOT NULL,
    number_in_stock INTEGER NOT NULL,
    bar_id          INTEGER NOT NULL 
);

CREATE TABLE orders (
    order_id        INTEGER NOT NULL PRIMARY KEY,
    order_datetime  TEXT    NOT NULL,
    drink_id        INTEGER NOT NULL,
    bar_id          INTEGER NOT NULL,
    order_amount    REAL    NOT NULL
)
