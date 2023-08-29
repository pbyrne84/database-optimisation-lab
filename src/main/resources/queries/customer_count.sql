-- https://sskelkar.medium.com/understanding-a-postgres-query-plan-75e8e1dc7d35

-- Count of all customers
SELECT count(1) FROM customer;

-- Count of customers without an order using a join - 49861111
-- runs faster than a subquery without an index - 20s
SELECT count(Distinct (customer.id))
FROM customer
         LEFT JOIN customer_order ON customer.id = customer_order.customer_id
WHERE customer_order.id IS NULL;

/*
 Aggregate  (cost=1031455.16..1031455.17 rows=1 width=8)
  ->  Gather  (cost=12395.51..1031455.16 rows=1 width=4)
        Workers Planned: 2
        ->  Parallel Hash Left Join  (cost=11395.51..1030455.06 rows=1 width=4)
              Hash Cond: (customer.id = customer_order.customer_id)
              Filter: (customer_order.id IS NULL)
              ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=4)
              ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=8)
                    ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=8)
JIT:
  Functions: 13
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"

 */

-- Count of customers without an order using a subquery
-- without and index this does not finish for a long time - gave up running it
SELECT count(id)
FROM customer
WHERE id NOT IN (SELECT customer_id FROM customer_order);

/*
 Limit  (cost=1276345.47..1276345.47 rows=1 width=37)
  ->  Sort  (cost=1276345.47..1278081.61 rows=694454 width=37)
"        Sort Key: (count(customer_order.id)) DESC, customer.created"
        ->  Finalize GroupAggregate  (cost=1179888.77..1265928.66 rows=694454 width=37)
"              Group Key: customer.id, customer.name, customer.created"
              ->  Gather Merge  (cost=1179888.77..1253197.00 rows=578712 width=37)
                    Workers Planned: 2
                    ->  Partial GroupAggregate  (cost=1178888.74..1185399.25 rows=289356 width=37)
"                          Group Key: customer.id, customer.name, customer.created"
                          ->  Sort  (cost=1178888.74..1179612.13 rows=289356 width=33)
"                                Sort Key: customer.id, customer.name, customer.created"
                                ->  Parallel Hash Join  (cost=11395.51..1144727.06 rows=289356 width=33)
                                      Hash Cond: (customer.id = customer_order.customer_id)
                                      ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=29)
                                      ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=8)
                                            ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=8)
JIT:
  Functions: 19
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"

 */


-- Count of customers with an order using a join - 138889
-- without and index runs slower than a subquery - about 12 seconds before optimisation
SELECT count(distinct (customer.id))
FROM customer
         LEFT JOIN customer_order ON customer.id = customer_order.customer_id
WHERE customer_order.id IS NOT NULL;

/*
 Aggregate  (cost=1102636.59..1102636.60 rows=1 width=8)
  ->  Gather  (cost=12395.51..1100900.46 rows=694454 width=4)
        Workers Planned: 2
        ->  Parallel Hash Join  (cost=11395.51..1030455.06 rows=289356 width=4)
              Hash Cond: (customer.id = customer_order.customer_id)
              ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=4)
              ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=4)
                    ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=4)
                          Filter: (id IS NOT NULL)
JIT:
  Functions: 13
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"

 */

-- Count of customers with an order using a subquery - 10 seconds
SELECT count(id)
FROM customer
WHERE id IN (SELECT customer_id FROM customer_order);

/*
 Finalize Aggregate  (cost=875802.65..875802.66 rows=1 width=8)
  ->  Gather  (cost=875802.43..875802.64 rows=2 width=8)
        Workers Planned: 2
        ->  Partial Aggregate  (cost=874802.43..874802.44 rows=1 width=8)
              ->  Hash Join  (cost=56096.75..874679.94 rows=48998 width=4)
                    Hash Cond: (customer.id = customer_order.customer_id)
                    ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=4)
                    ->  Hash  (cost=54626.81..54626.81 rows=117595 width=4)
                          ->  HashAggregate  (cost=48025.44..54626.81 rows=117595 width=4)
                                Group Key: customer_order.customer_id
                                ->  Seq Scan on customer_order  (cost=0.00..10698.54 rows=694454 width=4)
JIT:
  Functions: 16
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
 */


-- Customer with highest amount of orders and lowest amount of orders above 0, the oldest customers are preferred over new ones.
--  order_count DESC - 38 seconds
SELECT id, name, created, order_count
FROM (SELECT customer.id              AS id,
             customer.name            AS name,
             customer.created         AS created,
             count(customer_order.id) AS order_count
      FROM customer
               JOIN customer_order ON customer.id = customer_order.customer_id
      GROUP BY customer.id, customer.name, customer.created) as highest_buyer
ORDER BY order_count DESC, created ASC
LIMIT 1;
/*
 Limit  (cost=1276345.47..1276345.47 rows=1 width=37)
  ->  Sort  (cost=1276345.47..1278081.61 rows=694454 width=37)
"        Sort Key: (count(customer_order.id)) DESC, customer.created"
        ->  Finalize GroupAggregate  (cost=1179888.77..1265928.66 rows=694454 width=37)
"              Group Key: customer.id, customer.name, customer.created"
              ->  Gather Merge  (cost=1179888.77..1253197.00 rows=578712 width=37)
                    Workers Planned: 2
                    ->  Partial GroupAggregate  (cost=1178888.74..1185399.25 rows=289356 width=37)
"                          Group Key: customer.id, customer.name, customer.created"
                          ->  Sort  (cost=1178888.74..1179612.13 rows=289356 width=33)
"                                Sort Key: customer.id, customer.name, customer.created"
                                ->  Parallel Hash Join  (cost=11395.51..1144727.06 rows=289356 width=33)
                                      Hash Cond: (customer.id = customer_order.customer_id)
                                      ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=29)
                                      ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=8)
                                            ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=8)
JIT:
  Functions: 19
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
*/


-- Customer with lowers amount of orders and lowest amount of orders above 0, the oldest customers are preferred over new ones.
--  order_count ASC
SELECT id, name, created, order_count
FROM (SELECT customer.id              AS id,
             customer.name            AS name,
             customer.created         AS created,
             count(customer_order.id) AS order_count
      FROM customer
               JOIN customer_order ON customer.id = customer_order.customer_id
      GROUP BY customer.id, customer.name, customer.created) as highest_buyer
ORDER BY order_count ASC, created ASC
LIMIT 1;

/*
 Limit  (cost=1276345.47..1276345.47 rows=1 width=37)
  ->  Sort  (cost=1276345.47..1278081.61 rows=694454 width=37)
"        Sort Key: (count(customer_order.id)), customer.created"
        ->  Finalize GroupAggregate  (cost=1179888.77..1265928.66 rows=694454 width=37)
"              Group Key: customer.id, customer.name, customer.created"
              ->  Gather Merge  (cost=1179888.77..1253197.00 rows=578712 width=37)
                    Workers Planned: 2
                    ->  Partial GroupAggregate  (cost=1178888.74..1185399.25 rows=289356 width=37)
"                          Group Key: customer.id, customer.name, customer.created"
                          ->  Sort  (cost=1178888.74..1179612.13 rows=289356 width=33)
"                                Sort Key: customer.id, customer.name, customer.created"
                                ->  Parallel Hash Join  (cost=11395.51..1144727.06 rows=289356 width=33)
                                      Hash Cond: (customer.id = customer_order.customer_id)
                                      ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=29)
                                      ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=8)
                                            ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=8)
JIT:
  Functions: 19
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
 */



-- Single query that does both of the above with a union
(SELECT *
 FROM (SELECT customer.id              AS id,
              customer.name            AS name,
              customer.created         AS created,
              count(customer_order.id) AS order_count
       FROM customer
                JOIN customer_order ON customer.id = customer_order.customer_id
       GROUP BY customer.id, customer.name, customer.created) as highest_buyer
 ORDER BY order_count DESC, created ASC
 LIMIT 1)
UNION
(SELECT *
 FROM (SELECT customer.id              AS id,
              customer.name            AS name,
              customer.created         AS created,
              count(customer_order.id) AS order_count
       FROM customer
                JOIN customer_order ON customer.id = customer_order.customer_id
       GROUP BY customer.id, customer.name, customer.created) as highest_buyer
 ORDER BY order_count ASC, created ASC
 LIMIT 1);

-- customer with the newest order, use id as secondary. We could just use id assuming id insert order but lets be safe - 499996
-- we don't need the order id so we can skip using a join. 5s
SELECT id, name
FROM customer
WHERE id = ( SELECT customer_order.customer_id
             FROM customer_order
             ORDER BY customer_order.created DESC , customer_order.id DESC LIMIT 1);

/*
 Gather  (cost=10094.48..825694.83 rows=1 width=21)
  Workers Planned: 2
  Params Evaluated: $1
  InitPlan 1 (returns $1)
    ->  Limit  (cost=9094.36..9094.48 rows=1 width=16)
          ->  Gather Merge  (cost=9094.36..76615.48 rows=578712 width=16)
                Workers Planned: 2
                ->  Sort  (cost=8094.34..8817.73 rows=289356 width=16)
"                      Sort Key: customer_order.created DESC, customer_order.id DESC"
                      ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=16)
  ->  Parallel Seq Scan on customer  (cost=0.00..815600.25 rows=1 width=21)
        Filter: (id = $1)
JIT:
  Functions: 7
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
*/


-- Uses a join as we want to get the order id out with the created. 605 ms
SELECT customer.id, customer.name, customer_order.id, customer_order.created
FROM customer
         JOIN customer_order ON customer.id = customer_order.customer_id
ORDER BY  customer_order.created DESC, customer_order.id DESC
LIMIT 1;

/*
 Limit  (cost=38841.27..1015195.81 rows=1 width=33)
  ->  Nested Loop  (cost=38841.27..678033356211.72 rows=694454 width=33)
        Join Filter: (customer.id = customer_order.customer_id)
        ->  Gather Merge  (cost=38841.27..119721.88 rows=694454 width=16)
              Workers Planned: 2
              ->  Sort  (cost=37841.24..38564.63 rows=289356 width=16)
"                    Sort Key: customer_order.created DESC, customer_order.id DESC"
                    ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=16)
        ->  Materialize  (cost=0.00..1548169.76 rows=46806384 width=21)
              ->  Seq Scan on customer  (cost=0.00..1039880.84 rows=46806384 width=21)
JIT:
  Functions: 9
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
 */


-- Newest customer with an order using subquery - 8 seconds
SELECT id, name
FROM customer
WHERE id IN ( SELECT customer_order.customer_id
             FROM customer_order)
ORDER BY created DESC LIMIT 1;

/*
 Limit  (cost=834908.76..834908.88 rows=1 width=29)
  ->  Gather Merge  (cost=834908.76..846342.43 rows=97996 width=29)
        Workers Planned: 2
        ->  Sort  (cost=833908.74..834031.23 rows=48998 width=29)
              Sort Key: customer.created DESC
              ->  Hash Join  (cost=15080.56..833663.75 rows=48998 width=29)
                    Hash Cond: (customer.id = customer_order.customer_id)
                    ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=29)
                    ->  Hash  (cost=13610.63..13610.63 rows=117595 width=4)
                          ->  HashAggregate  (cost=12434.68..13610.63 rows=117595 width=4)
                                Group Key: customer_order.customer_id
                                ->  Seq Scan on customer_order  (cost=0.00..10698.54 rows=694454 width=4)
JIT:
  Functions: 13
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
 */


-- Newest customer with an order using a join - 22 seconds
SELECT customer.id, customer.name
FROM customer
JOIN customer_order ON customer.id = customer_order.customer_id
ORDER BY customer.created DESC
LIMIT 1;

/*
 Limit  (cost=1147173.86..1147173.98 rows=1 width=29)
  ->  Gather Merge  (cost=1147173.86..1214694.98 rows=578712 width=29)
        Workers Planned: 2
        ->  Sort  (cost=1146173.84..1146897.23 rows=289356 width=29)
              Sort Key: customer.created DESC
              ->  Parallel Hash Join  (cost=11395.51..1144727.06 rows=289356 width=29)
                    Hash Cond: (customer.id = customer_order.customer_id)
                    ->  Parallel Seq Scan on customer  (cost=0.00..766843.60 rows=19502660 width=29)
                    ->  Parallel Hash  (cost=6647.56..6647.56 rows=289356 width=4)
                          ->  Parallel Seq Scan on customer_order  (cost=0.00..6647.56 rows=289356 width=4)
JIT:
  Functions: 10
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"

 */

-- Newest customer with an order using a join with a group by, much faster than without a group by in this case
SELECT customer.id, customer.name
FROM  customer
 JOIN customer_order ON customer.id = customer_order.customer_id
GROUP BY customer.id,customer.name, customer.created
ORDER BY customer.created DESC
LIMIT 1;


-- Newest customer with an order using a join with the total price of said order - doesn't finish
SELECT customer.id, customer.name,customer.created, SUM(item.price)
FROM customer
         JOIN customer_order ON customer.id = customer_order.customer_id
         JOIN customer_order_item ON customer_order.id = customer_order_item.order_id
         JOIN item ON customer_order_item.item_id = item.id
GROUP BY customer.id, customer.name, customer.created
ORDER BY customer.created DESC
LIMIT 1;

-- Newest customer with an order using a join with a subquery with a group by. Can be multiple times faster than
-- the above query. The subquery is quite performant and limits the adventure the database can do while running.
SELECT customer.id, customer.name, customer.created, SUM(item.price)
FROM customer
         JOIN customer_order ON customer.id = customer_order.customer_id
         JOIN customer_order_item ON customer_order.id = customer_order_item.order_id
         JOIN item ON customer_order_item.item_id = item.id
WHERE customer.id = (SELECT customer.id
                     FROM customer
                              JOIN customer_order ON customer.id = customer_order.customer_id
                              JOIN customer_order_item ON customer_order.id = customer_order_item.order_id
                     GROUP BY customer.id, customer.name, customer.created
                     ORDER BY customer.created DESC
                     LIMIT 1)
GROUP BY customer.id, customer.name, customer.created;

