
ALTER TABLE customer ADD PRIMARY KEY (id);
ALTER TABLE item ADD PRIMARY KEY (id);
ALTER TABLE customer_order ADD PRIMARY KEY (id);

ALTER TABLE customer_order
    ADD CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customer(id);

ALTER TABLE customer_order_item
    ADD CONSTRAINT fk_customer_order_item_order_id FOREIGN KEY (order_id) REFERENCES customer(id);

ALTER TABLE customer_order_item
    ADD CONSTRAINT fk_customer_order_item_item_id FOREIGN KEY (item_id) REFERENCES customer(id);




