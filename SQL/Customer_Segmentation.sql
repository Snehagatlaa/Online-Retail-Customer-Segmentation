-- Customer Segmentation SQL Script
CREATE DATABASE Online_Retail_CustomerDB;
USE Online_Retail_CustomerDB;
-- ===============================================
-- 1. Create the main table for transactions
-- ===============================================
CREATE TABLE IF NOT EXISTS RetailData (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INTEGER,
    InvoiceDate TEXT,
    UnitPrice REAL,
    CustomerID INTEGER,
    Country TEXT
);
-- Load data from CSV
-- .mode csv
-- .import OnlineRetail_utf8.csv RetailData
ALTER TABLE RetailData
MODIFY Description VARCHAR(255) CHARACTER SET utf8mb4,
MODIFY Country VARCHAR(50) CHARACTER SET utf8mb4;
LOAD DATA INFILE '/path/to/OnlineRetail_utf8.csv'
    INTO TABLE RetailData
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, @CustomerID, Country)
    SET CustomerID = NULLIF(@CustomerID, '');
-- ===============================================
-- 2. Clean Data: Remove invalid entries
-- ===============================================
-- Create a cleaned table
CREATE TABLE IF NOT EXISTS RetailData_Clean AS
SELECT *
FROM RetailData
WHERE CustomerID IS NOT NULL
  AND Quantity > 0
  AND UnitPrice > 0;
-- ===============================================
-- 3. Aggregate Customer-Level Data
-- ===============================================
CREATE VIEW CustomerAggregates AS
SELECT
    CustomerID,
    COUNT(DISTINCT InvoiceNo) AS Frequency,         -- Total number of orders
    SUM(Quantity * UnitPrice) AS TotalSpend,       -- Total spend
    AVG(Quantity * UnitPrice) AS AvgOrderValue,    -- Average order value
    MIN(InvoiceDate) AS FirstPurchase,
    MAX(InvoiceDate) AS LastPurchase
FROM RetailData_Clean
GROUP BY CustomerID;
-- ===============================================
-- 4. Calculate Recency (in days)
-- ===============================================
-- Replace '2025-09-10' with CURRENT_DATE(if supported)
CREATE VIEW CustomerRFM AS
SELECT
    CustomerID,
    TotalSpend,
    Frequency,
    AvgOrderValue,
    FirstPurchase,
    LastPurchase,
    DATEDIFF(CURDATE(), LastPurchase) AS Recency
FROM CustomerAggregates;
-- ===============================================
-- 5. Calculate Customer Lifetime Value (CLV)
-- ===============================================
-- Assuming average margin of 30%
CREATE VIEW CustomerCLV AS
SELECT
    CustomerID,
    TotalSpend,
    Frequency,
    AvgOrderValue,
    Recency,
    TotalSpend * 0.3 * Frequency / NULLIF(Frequency, 0) AS CLV
FROM CustomerRFM;
-- ===============================================
-- 6. Prepare Data for Clustering
-- ===============================================
CREATE VIEW CustomerClustering AS
SELECT
    CustomerID,
    TotalSpend,
    Frequency,
    Recency,
    AvgOrderValue,
    CLV
FROM CustomerCLV;
-- ===============================================
-- 7. Detect anomalies (flag high spenders or unusual patterns)
-- ===============================================
-- Flag customers with TotalSpend > 95th percentile as "High Value"
CREATE TABLE Threshold AS
SELECT TotalSpend AS ThresholdValue
FROM CustomerClustering AS cc1
WHERE (
    SELECT COUNT(*)
    FROM CustomerClustering AS cc2
    WHERE cc2.TotalSpend > cc1.TotalSpend
 ) = FLOOR((SELECT COUNT(*) FROM CustomerClustering) * 0.05);
CREATE VIEW CustomerAnomalies AS
SELECT cc.*,
       CASE
           WHEN cc.TotalSpend > (SELECT ThresholdValue FROM Threshold) THEN 'High Value' 
           ELSE 'Normal'                         
       END AS Anomaly
FROM CustomerClustering AS cc;
-- ===============================================
-- 8. Market Basket Analysis Preparation
-- ===============================================
CREATE VIEW CustomerProducts AS
SELECT
    CustomerID,
    Description,
    SUM(Quantity) AS TotalQuantity
FROM RetailData_Clean
GROUP BY CustomerID, Description;
