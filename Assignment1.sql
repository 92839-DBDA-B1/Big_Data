-- Assignment1

-- Customers
create table Customers(
CustomerId int, 
Name varchar(100),
age int, 
locationId int
);

INSERT INTO Customers VALUES (1, 'John Doe', 30, 1);
INSERT INTO Customers VALUES (2, 'Jane Smith', 25, 2);
INSERT INTO Customers VALUES (3, 'Bob Johnson', 35, 1);
INSERT INTO Customers VALUES (4, 'Alice Brown', 28, 3);
INSERT INTO Customers VALUES (5, 'Charlie Davis', 32, 2);

-------------------------------------------------------------
-- Products	
create table Products(
productId int, 
productname varchar(100), 
category varchar(50),
price decimal(10,2)
);

INSERT INTO Products VALUES (1, 'Laptop', 'Electronics', 800.00);
INSERT INTO Products VALUES (2, 'Smartphone', 'Electronics', 400.00);
INSERT INTO Products VALUES (3, 'T-shirt', 'Clothing', 20.00);
INSERT INTO Products VALUES (4, 'Shoes', 'Footwear', 50.00);
INSERT INTO Products VALUES (5, 'Bookshelf', 'Furniture', 150.00);

-------------------------------------------------------------
-- Sales
create table sales(
saleid int,
customerid int,
productid int, 
saledate date,
quantity int, 
totalamount decimal(10,2)
);

INSERT INTO Sales VALUES (1, 1, 1, '2023-01-01', 2, 1600.00);
INSERT INTO Sales VALUES (2, 2, 3, '2023-01-02', 3, 60.00);
INSERT INTO Sales VALUES (3, 3, 2, '2023-01-03', 1, 400.00);
INSERT INTO Sales VALUES (4, 4, 4, '2023-02-01', 2, 100.00);
INSERT INTO Sales VALUES (5, 5, 5, '2023-02-02', 1, 150.00);

--------------------------------------------------------------
-- Locations
create table locations(
Locationid int,
city varchar(50),
state varchar(50)
);

INSERT INTO locations VALUES (1, 'Pune', 'Maharashtra');
INSERT INTO locations VALUES (2, 'Mumbai', 'Maharashtra');
INSERT INTO locations VALUES (3, 'Bangalore', 'Karnataka');
INSERT INTO locations VALUES (4, 'Delhi', 'Delhi');
INSERT INTO locations VALUES (5, 'Chennai', 'Tamil Nadu');

-------------------------------------------------------------
-- A. Retrieve the names of all customers who made a purchase.
SELECT c.name
FROM Customers AS c
JOIN Sales AS s
ON c.customerid = s.customerid;

-- Output
+----------------+
|     c.name     |
+----------------+
| John Doe       |
| Jane Smith     |
| Bob Johnson    |
| Alice Brown    |
| Charlie Davis  |
+----------------+
5 rows selected (20.116 seconds)

-------------------------------------------------------------
-- B. List the products and their total sales amounts for a given date range.

SELECT p.productname, SUM(s.totalamount) AS totalsales
FROM products AS p
JOIN sales AS s
ON p.productid = s.productid
WHERE saledate BETWEEN '2023-01-01' AND '2023-01-31'
GROUP BY p.productname;
-- Output
+----------------+-------------+
| p.productname  | totalsales  |
+----------------+-------------+
| Laptop         | 1600.00     |
| Smartphone     | 400.00      |
| T-shirt        | 60.00       |
+----------------+-------------+
3 rows selected (23.88 seconds)

-------------------------------------------------------------
-- C. Find the total sales amount for each product category.
SELECT p.category, SUM(totalamount) AS salesamt
FROM products AS p
JOIN sales AS s
ON P.productid = S.productid
GROUP BY p.category;
-- Output
+--------------+-----------+
|  p.category  | salesamt  |
+--------------+-----------+
| Clothing     | 60.00     |
| Electronics  | 2000.00   |
| Footwear     | 100.00    |
| Furniture    | 150.00    |
+--------------+-----------+
4 rows selected (25.729 seconds)

-------------------------------------------------------------
--D. Identify the customers who made purchases in a speciﬁc city.
SELECT c.name, l.city
FROM customers AS c
JOIN locations AS l
ON c.locationId = l.locationId;
-- Output
+----------------+------------+
|     c.name     |   l.city   |
+----------------+------------+
| John Doe       | Pune       |
| Bob Johnson    | Pune       |
| Jane Smith     | Mumbai     |
| Charlie Davis  | Mumbai     |
| Alice Brown    | Bangalore  |
+----------------+------------+
5 rows selected (19.321 seconds)

-------------------------------------------------------------
-- E. Calculate the average age of customers who bought products in the 'Electronics' category.
SELECT AVG(c.age)
FROM customers AS c
JOIN sales AS s
ON s.customerid = c.customerid
JOIN products AS p
ON s.productid = p.productid
WHERE p.category ='Electronics';
-- Output
+-------+
|  _c0  |
+-------+
| 32.5  |
+-------+
1 row selected (26.279 seconds)

-------------------------------------------------------------
-- F. List the top 3 products based on total sales amount.
SELECT p.productname,s.totalamount
FROM products AS p
JOIN sales AS s
ON p.productid = s.productid
ORDER BY s.totalamount DESC
LIMIT 3;

+----------------+----------------+
| p.productname  | s.totalamount  |
+----------------+----------------+
| Laptop         | 1600.00        |
| Smartphone     | 400.00         |
| Bookshelf      | 150.00         |
+----------------+----------------+
3 rows selected (23.432 seconds)

-------------------------------------------------------------
-- G. Find the total sales amount for each month.
SELECT MONTH(s.saledate) AS salesmonth,SUM(s.totalamount) AS monthlytotal
FROM sales AS s
JOIN sales AS s1
ON s.saleid = s1.saleid
GROUP BY MONTH(s.saledate);

+-------------+---------------+
| salesmonth  | monthlytotal  |
+-------------+---------------+
| 1           | 2060.00       |
| 2           | 250.00        |
+-------------+---------------+
2 rows selected (21.732 seconds)

-------------
SELECT
  DATE_FORMAT(SaleDate, 'yyyy-MM') AS Month,
  SUM(TotalAmount) AS TotalSales
FROM sales
GROUP BY DATE_FORMAT(SaleDate, 'yyyy-MM')
ORDER BY Month;

+----------+-------------+
|  month   | totalsales  |
+----------+-------------+
| 2023-01  | 2060.00     |
| 2023-02  | 250.00      |
+----------+-------------+
2 rows selected (41.922 seconds)

-------------------------------------------------------------
-- H. Identify the products with no sales.
SELECT p.ProductName
FROM Products p
LEFT JOIN Sales s ON p.ProductID = s.ProductID
WHERE s.SaleID IS NULL;

+----------------+
| p.productname  |
+----------------+
+----------------+
No rows selected (18.356 seconds)

-------------------------------------------------------------
-- I. Calculate the total sales amount for each state.
SELECT l.state, SUM(s.totalamount) AS statetotal
FROM sales AS s
JOIN customers AS c
ON c.customerid = s.customerid
JOIN locations AS l
ON l.locationid = c.locationid
GROUP BY l.state;

+--------------+-------------+
|   l.state    | statetotal  |
+--------------+-------------+
| Karnataka    | 100.00      |
| Maharashtra  | 2210.00     |
+--------------+-------------+
2 rows selected (23.02 seconds)

-------------------------------------------------------------
-- J. Retrieve the customer names and their highest purchase amount.

SELECT c.Name, MAX(s.TotalAmount) AS MaxPurchase
FROM Sales s
JOIN Customers c ON s.CustomerID = c.CustomerID
GROUP BY c.Name;

+----------------+--------------+
|     c.name     | maxpurchase  |
+----------------+--------------+
| Alice Brown    | 100.00       |
| Bob Johnson    | 400.00       |
| Charlie Davis  | 150.00       |
| Jane Smith     | 60.00        |
| John Doe       | 1600.00      |
+----------------+--------------+
5 rows selected (23.677 seconds)

-------------------------------------------------------------
