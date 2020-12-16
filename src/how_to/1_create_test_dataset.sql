-- this test dataset contains two source tables, customer (JSON data) and sale (regular flat data). 
-- their data will be processed by the framework into hubs, link and satellites

CREATE SCHEMA IF NOT EXISTS tst;

CREATE TABLE tst.customer (
  record_content VARIANT,
  record_metadata VARIANT
);


CREATE TABLE tst.sale (
  sale_id integer,
  sale_date date,
  customer_id integer,
  total_price numeric(10,2)
);


INSERT INTO tst.customer (record_content, record_metadata)
select parse_json($1):content, parse_json($1):meta
  from values 
  ('{
      "meta":
      {
          "offset": 1,
          "topic": "CustomerInfo",
          "partition": 1,
          "key": "customerId",
          "LogAppendTime": 1607698894,
      },
      "content":
      {
          "customerId": 1,
          "surname": "Power",
          "name": "Max",
          "address": {    
            "zipCode": "12345",
            "street": "Berliner Str.",
            "houseNumber": "99"
          }
      }
    }'), ('{
      "meta":
      {
          "offset": 2,
          "topic": "CustomerInfo",
          "partition": 1,
          "key": "customerId",
          "LogAppendTime": 1607698898,
      },
      "content":
      {
          "customerId": 2,
          "surname": "Simpson",
          "name": "Maggie",
          "address": {
            "zipCode": "54321",
            "street": "Leipziger Str.",
            "houseNumber": "55"
          }
      }            
    }'), ('{
      "meta":
      {
          "offset": 3,
          "topic": "CustomerInfo",
          "partition": 1,
          "key": "customerId",
          "LogAppendTime": 1607698996,
      },
      "content":
      {
          "customerId": 1,
          "surname": "Power",
          "name": "Max",
          "address": {          
            "zipCode": "12345",
            "street": "Hamburger Str.",
            "houseNumber": "33"
          }
      }                
    }');
    
INSERT INTO tst.sale VALUES
(100,'2020-01-06',1,59.90),
(200,'2020-01-07',2,20.99),
(300,'2020-01-08',2,9.49);