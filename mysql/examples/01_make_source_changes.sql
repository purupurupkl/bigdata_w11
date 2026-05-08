USE classicmodels;

-- Update: change the selling price for an existing order line.
UPDATE orderdetails
SET priceEach = 145.00
WHERE orderNumber = 10100
  AND productCode = 'S18_1749';

-- Insert: add a new shipped order and line item.
INSERT INTO orders (
  orderNumber,
  orderDate,
  requiredDate,
  shippedDate,
  status,
  comments,
  customerNumber
) VALUES (
  10106,
  '2003-02-17',
  '2003-02-24',
  '2003-02-21',
  'Shipped',
  'Inserted after the first lakehouse sync.',
  112
);

INSERT INTO orderdetails (
  orderNumber,
  productCode,
  quantityOrdered,
  priceEach,
  orderLineNumber
) VALUES (
  10106,
  'S10_1678',
  12,
  95.70,
  1
);

-- Delete: remove one old order line to prove the next sync marks it deleted in bronze.
DELETE FROM orderdetails
WHERE orderNumber = 10101
  AND productCode = 'S18_2248';
