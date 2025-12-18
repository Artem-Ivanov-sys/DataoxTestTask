CREATE TABLE IF NOT EXISTS cars (
    id SERIAL PRIMARY KEY,
    url TEXT,
    title TEXT,
    price_usd INT,
    odometer INT,
    username TEXT,
    phone_number BIGINT,
    image_url TEXT,
    image_count INT,
    car_number TEXT,
    car_vin TEXT,
    datetime_found TIMESTAMP NOT NULL
);