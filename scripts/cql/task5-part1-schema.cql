-- Keyspace з найпростішою стратегією реплікації: SimpleStrategy кладе копії
-- на наступні за кільцем вузли, не зважаючи на стійки й датацентри.
CREATE KEYSPACE IF NOT EXISTS shop
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
USE shop;

-- category є ключем партиції: усі товари категорії лежать в одній партиції,
-- тож пошук за категорією читає одну партицію. Усередині рядки впорядковані
-- за ціною (clustering column), id розрізняє товари з однаковою ціною.
CREATE TABLE IF NOT EXISTS items (
  category text,
  price decimal,
  id uuid,
  name text,
  producer text,
  props map<text, text>,
  PRIMARY KEY ((category), price, id)
) WITH CLUSTERING ORDER BY (price ASC, id ASC);

-- Індекси за вмістом map: за парами ключ-значення і за ключами.
CREATE INDEX IF NOT EXISTS items_props_entries ON items (ENTRIES(props));
CREATE INDEX IF NOT EXISTS items_props_keys ON items (KEYS(props));

-- Materialized views для пошуку за назвою і за виробником у межах категорії.
CREATE MATERIALIZED VIEW IF NOT EXISTS items_by_name AS
  SELECT * FROM items
  WHERE category IS NOT NULL AND name IS NOT NULL AND price IS NOT NULL AND id IS NOT NULL
  PRIMARY KEY ((category), name, price, id);
CREATE MATERIALIZED VIEW IF NOT EXISTS items_by_producer AS
  SELECT * FROM items
  WHERE category IS NOT NULL AND producer IS NOT NULL AND price IS NOT NULL AND id IS NOT NULL
  PRIMARY KEY ((category), producer, price, id);

-- Замовлення: ключ партиції це замовник, тож усі його замовлення лежать в
-- одній партиції, від найновішого.
CREATE TABLE IF NOT EXISTS orders (
  customer text,
  order_date timestamp,
  order_id uuid,
  item_ids list<uuid>,
  total decimal,
  delivery text,
  PRIMARY KEY ((customer), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id ASC);
