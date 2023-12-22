CREATE SCHEMA IF NOT EXISTS csv;

CREATE TABLE csv.addresses (
    address_id VARCHAR(256) NOT NULL,
    zipcode INTEGER,
    country VARCHAR(256),
    address VARCHAR(256),
    state VARCHAR(256),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (address_id)
);

CREATE TABLE csv.events (
    event_id VARCHAR(1024) NOT NULL,
    page_url VARCHAR(1024),
    event_type VARCHAR(100),
    user_id VARCHAR(1024),
    product_id VARCHAR(1024),
    session_id VARCHAR(1024),
    created_at TIMESTAMPTZ,
    order_id VARCHAR(1024),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id)
);

CREATE TABLE csv.orders (
    order_id VARCHAR(256) NOT NULL,
    shipping_service VARCHAR(256),
    shipping_cost FLOAT,
    address_id VARCHAR(256),
    created_at TIMESTAMPTZ,
    promo_id VARCHAR(256),
    estimated_delivery_at TIMESTAMPTZ,
    order_cost FLOAT,
    user_id VARCHAR(256),
    order_total FLOAT,
    delivered_at TIMESTAMPTZ,
    tracking_id VARCHAR(256),
    status VARCHAR(256),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id)
);

CREATE TABLE csv.order_items (
    order_id VARCHAR(256) NOT NULL,
    product_id VARCHAR(256) NOT NULL,
    quantity INTEGER,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE csv.products (
    product_id VARCHAR(256) NOT NULL,
    price FLOAT,
    name VARCHAR(256),
    inventory INTEGER,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);

CREATE TABLE csv.promos (
    promo_id VARCHAR(256) NOT NULL,
    discount INTEGER,
    status VARCHAR(256),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (promo_id)
);

CREATE TABLE csv.users (
    user_id VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ,
    address_id VARCHAR(256),
    last_name VARCHAR(256),
    created_at TIMESTAMPTZ,
    phone_number VARCHAR(256),
    total_orders INTEGER,
    first_name VARCHAR(256),
    email VARCHAR(256),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

CREATE TABLE csv.budget (
    _row INTEGER NOT NULL,
    quantity INTEGER,
    month DATE,
    product_id VARCHAR(256),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (_row)
);
