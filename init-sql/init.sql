CREATE TABLE `customer_tab` (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL
);
CREATE TABLE `order_tab` (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id BIGINT NOT NULL,
    order_time BIGINT NOT NULL,
    create_time BIGINT NOT NULL
);
CREATE TABLE `customer_preference_tab` (
    customer_id BIGINT PRIMARY KEY,
    frequency INT NOT NULL COMMENT 'days'
);
CREATE TABLE `customer_reorder_tab` (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL DEFAULT '',
    last_name VARCHAR(256) NOT NULL DEFAULT '',
    order_count INT NOT NULL DEFAULT '0',
    last_order_time BIGINT NOT NULL DEFAULT '0',
    expected_next_order_time BIGINT NOT NULL DEFAULT '0'
);