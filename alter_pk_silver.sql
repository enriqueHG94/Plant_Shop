ALTER TABLE csv.trf_addresses ADD PRIMARY KEY (id_address);
ALTER TABLE csv.trf_budget ADD PRIMARY KEY (_row);
ALTER TABLE csv.trf_events ADD PRIMARY KEY (id_event);
ALTER TABLE csv.trf_order_items ADD PRIMARY KEY (id_order, id_product);
ALTER TABLE csv.trf_orders ADD PRIMARY KEY (id_order);
ALTER TABLE csv.trf_products ADD PRIMARY KEY (id_product);
ALTER TABLE csv.trf_promos ADD PRIMARY KEY (id_promo);
ALTER TABLE csv.trf_users ADD PRIMARY KEY (id_user);