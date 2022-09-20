CREATE TABLE poc_table
AS
SELECT date(o.order_datetime) AS 'order_date', o.bar_id, d.glass_id, COUNT(d.glass_id) AS glasses_count
FROM orders o
LEFT JOIN drinks d
ON o.drink_id=d.drink_id
GROUP BY order_date, bar_id;