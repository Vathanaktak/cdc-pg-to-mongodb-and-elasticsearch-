CREATE TABLE customers (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT UNIQUE NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id           SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers(id),
  status       TEXT NOT NULL DEFAULT 'NEW',
  total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- one-to-many: orders 1 --- N order_items
CREATE TABLE order_items (
  id           SERIAL PRIMARY KEY,
  order_id     INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  sku          TEXT NOT NULL,
  product_name TEXT NOT NULL,
  quantity     INT NOT NULL,
  unit_price   NUMERIC(12,2) NOT NULL
);

-- UPDATE/DELETE events must carry the full "before" row (needed for order_id on item deletes)
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

INSERT INTO customers (name,email) VALUES ('Alice','alice@example.com'),('Bob','bob@example.com');
INSERT INTO orders (customer_id,status,total_amount) VALUES (1,'NEW',45.00);
INSERT INTO order_items (order_id,sku,product_name,quantity,unit_price)
VALUES (1,'SKU-1','Keyboard',1,25.00),(1,'SKU-2','Mouse',1,20.00);
