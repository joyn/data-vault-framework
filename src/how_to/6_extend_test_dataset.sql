-- make sure all the tasks are running (see step 5). 

-- insert more data into customer and sale table to see how it is processed by the tasks
INSERT INTO tst.customer (record_content, record_metadata)
select parse_json($1):content, parse_json($1):meta
  from values 
('{
      "meta":
      {
          "offset": 4,
          "topic": "CustomerInfo",
          "partition": 1,
          "key": "customerId",
          "LogAppendTime": 1607699990,
      },
      "content":
      {
          "customerId": 3,
          "surname": "Gumble",
          "name": "Barney",
          "address": {          
            "zipCode": "80796",
            "city": "Munich",
            "street": "Frankfurter Str.",
            "houseNumber": "22"
          }
      }                
    }');
    
    
INSERT INTO tst.sale VALUES
(100,'2020-01-06',1,49.90),
(400,'2020-01-15',2,15.49),
(400,'2020-01-15',2,15.49),
(500,'2020-01-16',3,29.99);  



-- Note: the tasks are scheduled every 10 minutes per default.
-- grap a coffee and relax, almost done :)
-- after task execution you should get the following results

-- 3 entries
SELECT * FROM edw.customer_h;
-- 5 entries
SELECT * FROM edw.sale_h;

-- 5 entries
SELECT * FROM edw.customer_sale_l;

-- 3 entries
SELECT * FROM edw.customer_name_s;
-- 4 entries
SELECT * FROM edw.customer_address_s;
-- 6 entries
SELECT * FROM edw.sale_detail_s;