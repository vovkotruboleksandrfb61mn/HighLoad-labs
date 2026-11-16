#!/usr/bin/env bash
# Task 5, Part 1: the shop data model (items, orders) and its queries.
#
#   scripts/task5-part1.sh
#
# Drops and recreates the `shop` keyspace from scripts/cql/task5-part1-schema.cql,
# loads scripts/cql/task5-part1-data.cql, and runs the queries of the task in
# cqlsh sessions (scripts/cql-session.py), one transcript per topic:
#   $OUT_DIR/task5-part1-1.txt  the schema and DESCRIBE TABLE items
#   $OUT_DIR/task5-part1-2.txt  items: by category sorted by price, by name,
#                               price range, price and producer, map contents
#   $OUT_DIR/task5-part1-3.txt  orders: DESCRIBE, by customer, totals, WRITETIME
# With SHOTS=true (default) every transcript is rendered into $IMG_DIR as
# task5-part1-N[a-z].png pieces by scripts/terminal-shot.py.
#
# Starts the cluster if it is not running (and leaves it running).
# Overridable: OUT_DIR (reports/logs), IMG_DIR (reports/img), SHOTS (true).
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
if ! command -v cqlsh >/dev/null && [ -z "${HLS_CASSANDRA_REEXEC:-}" ] && command -v direnv >/dev/null; then
    HLS_CASSANDRA_REEXEC=1 exec direnv exec "$script_dir/.." "$0" "$@"
fi
. "$script_dir/task5-common.sh"

cluster_up
OUT_DIR=${OUT_DIR:-$HLS_ROOT/reports/logs}
IMG_DIR=${IMG_DIR:-$HLS_ROOT/reports/img}
SHOTS=${SHOTS:-true}
mkdir -p "$OUT_DIR"

cqlsh_quiet 1 -e 'DROP KEYSPACE IF EXISTS shop' >/dev/null

# 1. Schema.
out="$OUT_DIR/task5-part1-1.txt"
: >"$out"
{
    cat "$script_dir/cql/task5-part1-schema.cql"
    printf '\nDESCRIBE TABLE items\nDESCRIBE MATERIALIZED VIEW items_by_producer\n'
} | cql_session "$out" 1

cqlsh_quiet 1 -f "$script_dir/cql/task5-part1-data.cql"

# 2. Items.
out="$OUT_DIR/task5-part1-2.txt"
: >"$out"
cql_session "$out" 1 <<'CQL'
USE shop;

-- Усі телефони за зростанням ціни (порядок clustering column у партиції):
SELECT name, producer, price FROM items WHERE category = 'Телефони';
-- Те саме, від найдорожчого:
SELECT name, producer, price FROM items WHERE category = 'Телефони' ORDER BY price DESC;

-- За назвою в межах категорії, через view items_by_name:
SELECT category, name, producer, price FROM items_by_name WHERE category = 'Ноутбуки' AND name = 'ThinkPad T14';

-- Ціна в проміжку в межах категорії: зріз за clustering column price:
SELECT name, producer, price FROM items WHERE category = 'Ноутбуки' AND price >= 35000 AND price <= 50000;

-- Ціна та виробник, через view items_by_producer:
SELECT name, producer, price FROM items_by_producer WHERE category = 'Телефони' AND producer = 'Samsung' AND price < 15000;

-- За вмістом map, через індекси на props (пари ключ-значення і ключі):
SELECT name, price, props FROM items WHERE category = 'Телефони' AND props['колір'] = 'чорний';
SELECT name, price, props FROM items WHERE category = 'Телефони' AND props CONTAINS KEY 'eSIM';

-- Без ключа партиції запит мав би обійти весь кластер, тому Cassandra відмовляє:
SELECT name, price FROM items WHERE price > 40000;
CQL

# 3. Orders.
out="$OUT_DIR/task5-part1-3.txt"
: >"$out"
order_id=$(cqlsh_quiet 1 -e "SELECT order_id FROM shop.orders WHERE customer = 'Олена Коваль' AND order_date = '2026-10-21 18:40:00+0000'" | sed -n 's/^ *\([0-9a-f-]\{36\}\) *$/\1/p')
sleep 2  # so that the corrected price has a visibly later WRITETIME
sed "s/ORDER_ID/$order_id/" <<'CQL' | cql_session "$out" 1
USE shop;
DESCRIBE TABLE orders

-- Усі замовлення замовника, від найновішого (порядок clustering column):
SELECT order_date, total, delivery, item_ids FROM orders WHERE customer = 'Олена Коваль';
-- Від найстарішого:
SELECT order_date, total, delivery FROM orders WHERE customer = 'Олена Коваль' ORDER BY order_date ASC;

-- Загальна сума замовлень кожного замовника (GROUP BY за ключем партиції):
SELECT customer, COUNT(*) AS orders, SUM(total) AS spent FROM orders GROUP BY customer;

-- Пізніше ціну одного замовлення виправляють:
UPDATE orders SET total = 69998 WHERE customer = 'Олена Коваль' AND order_date = '2026-10-21 18:40:00+0000' AND order_id = ORDER_ID;

-- Коли записано ціну кожного замовлення (мікросекунди від 1970-01-01 UTC):
SELECT customer, order_date, total, WRITETIME(total) AS total_written FROM orders;
CQL

if $SHOTS; then
    shoot "$OUT_DIR/task5-part1-1.txt" "$IMG_DIR/task5-part1-1"
    shoot "$OUT_DIR/task5-part1-2.txt" "$IMG_DIR/task5-part1-2"
    shoot "$OUT_DIR/task5-part1-3.txt" "$IMG_DIR/task5-part1-3"
fi
