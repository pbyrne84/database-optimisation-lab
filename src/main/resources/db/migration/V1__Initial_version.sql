CREATE TABLE customer
(
    id      INT NOT NULL ,
    name    VARCHAR(200) NOT NULL ,
    created TIMESTAMP NOT NULL,
    updated TIMESTAMP NOT NULL
);

CREATE TABLE item
(
    id      INT NOT NULL,
    name    VARCHAR(200) NOT NULL,
    price   DECIMAL NOT NULL,
    created TIMESTAMP NOT NULL,
    updated TIMESTAMP NOT NULL
);

CREATE TABLE customer_order
(
    id          INT NOT NULL,
    customer_id INT NOT NULL,
    created     TIMESTAMP NOT NULL
);

CREATE TABLE customer_order_item
(
    order_id INT NOT NULL,
    item_id  INT NOT NULL
);




