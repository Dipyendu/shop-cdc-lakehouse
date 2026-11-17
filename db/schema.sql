-- Shop source schema. Ids are assigned by the generator (deterministic plans).
-- updated_at is set by the database: default on insert, trigger on update.
CREATE TABLE customers (
  customer_id  bigint PRIMARY KEY,
  name         text        NOT NULL,
  email        text        NOT NULL,
  city         text        NOT NULL,
  country      text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE products (
  product_id   bigint PRIMARY KEY,
  name         text          NOT NULL,
  category     text          NOT NULL,
  price        numeric(10,2) NOT NULL,
  updated_at   timestamptz   NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE orders (
  order_id     bigint PRIMARY KEY,
  customer_id  bigint      NOT NULL REFERENCES customers(customer_id),
  status       text        NOT NULL,
  ordered_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE order_items (
  order_item_id bigint PRIMARY KEY,
  order_id      bigint        NOT NULL REFERENCES orders(order_id),
  product_id    bigint        NOT NULL REFERENCES products(product_id),
  quantity      integer       NOT NULL,
  unit_price    numeric(10,2) NOT NULL,
  updated_at    timestamptz   NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE payments (
  payment_id   bigint PRIMARY KEY,
  order_id     bigint        NOT NULL REFERENCES orders(order_id),
  amount       numeric(12,2) NOT NULL,
  method       text          NOT NULL,
  status       text          NOT NULL,
  updated_at   timestamptz   NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := clock_timestamp(); RETURN NEW; END $$;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['customers','products','orders','order_items','payments'] LOOP
    EXECUTE format('CREATE TRIGGER %I_updated_at BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION set_updated_at()', t, t);
  END LOOP;
END $$;

-- Debezium (pgoutput) publishes these five tables only.
CREATE PUBLICATION shop_pub FOR TABLE customers, products, orders, order_items, payments;
