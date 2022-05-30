DROP TABLE IF EXISTS `customer_tab`;
CREATE TABLE `customer_tab` (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL
);
DROP TABLE IF EXISTS `order_tab`;
CREATE TABLE `order_tab` (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id BIGINT NOT NULL,
    order_time BIGINT NOT NULL,
    create_time BIGINT NOT NULL
);
DROP TABLE IF EXISTS `customer_preference_tab`;
CREATE TABLE `customer_preference_tab` (
    customer_id BIGINT PRIMARY KEY,
    frequency INT NOT NULL COMMENT 'days'
);
CREATE TABLE `customer_reorder_tab` (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    order_count INT NOT NULL DEFAULT '0',
    last_order_time BIGINT NOT NULL DEFAULT '0',
    expect_next_order_time BIGINT NOT NULL DEFAULT '0'
);