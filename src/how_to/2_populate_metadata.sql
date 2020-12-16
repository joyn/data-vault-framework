-- now we insert into the data vault framework metadata tables
-- note: as an alternative you can also use CSV files and load them into snowflake

-- src table identifier
INSERT INTO MTD.mtd_src_table VALUES 
 (1, CURRENT_DATABASE(), 'TST', 'customer', NULL, 'Contains all customers', 1),
 (2, CURRENT_DATABASE(), 'TST', 'sale', NULL, 'Contains all transactions', 1)
;

-- src column identifier and path (json)
INSERT INTO MTD.mtd_src_column VALUES 
  (1, 1, NULL,'record_content:customerId','integer', NULL,'Customer ID',1),
  (2, 1, NULL,'record_content:surname','varchar', NULL,'Last Name',1),
  (3, 1, NULL,'record_content:name','varchar', NULL,'First Name',1),
  (4, 1, NULL,'record_content:address','variant', NULL,'Full Customer Address Details',1),
  (5, 2, NULL,'sale_id','integer', NULL,'Sale ID',1),
  (6, 2, NULL,'sale_date','date', NULL,'Date of Sale',1),
  (7, 2, NULL,'customer_id','integer', NULL,'Customer ID',1),
  (8, 2, NULL,'total_price','numeric', NULL,'Total price of sale',1)  
;

-- hub
INSERT INTO MTD.mtd_hub VALUES 
  (1,'CUSTOMER_H',FALSE,1),
  (2,'SALE_H',FALSE,1)
;

INSERT INTO MTD.mtd_hub_map VALUES 
  (1,1,1,'CUSTOMER_ID',1,1),
  (2,7,1,'CUSTOMER_ID',1,1),
  (2,5,2,'SALE_ID',1,1)
;

-- link
INSERT INTO MTD.mtd_link VALUES 
  (1,'CUSTOMER_SALE_L',FALSE,FALSE,1)
;


INSERT INTO MTD.mtd_link_hub_ref VALUES 
  (1,2,1,1,1),
  (1,2,2,2,1)
;

-- sat
INSERT INTO MTD.mtd_sat VALUES 
  (1,'CUSTOMER_NAME_S',1,1,NULL,FALSE,TRUE,TRUE,1),
  (2,'CUSTOMER_ADDRESS_S',1,1,NULL,FALSE,TRUE,TRUE,1),
  (3,'SALE_DETAIL_S',2,2,NULL,FALSE,TRUE,FALSE,1)
;

INSERT INTO MTD.mtd_sat_map VALUES 
  (1,2,1,'last_name',1),
  (1,3,1,'first_name',1),
  (1,4,2,'address',1),
  (2,6,3,'sale_date',1),
  (2,8,3,'price',1)
;

-- approve usage of the above by inserting into the log
INSERT INTO MTD.MTD_MODEL_CREATION_LOG VALUES (1, 1, TRUE, to_timestamp_ntz(current_timestamp));