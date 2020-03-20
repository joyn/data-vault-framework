USE SCHEMA MTD;
CREATE OR REPLACE VIEW MTD.MTD_HASH_KEY AS
SELECT  
map.table_name, map.src_table_id, map.version_id,LISTAGG(src_c.array_path) as array_path, LISTAGG(src_c.FILTER_CRITERIA) as FILTER_CRITERIA,
'upper(md5(upper(' ||
LISTAGG(CAST('NVL(TRIM( src.'||
             IFF(src_c.array_path is not null,'value'|| IFF(src_column is not null,':'||TRIM(src_column),'')
                   ,IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,'')))
             ||',\\\'"\\\'),\\\'\\\')' AS VARCHAR), ' ||\\\'\;\\\'|| ')
within group (ORDER BY hub_order_id, NVL(HUB_COLUMN_ORDER_ID,1) ASC)
     ||')))' AS ddl ,
'not (' 
    || LISTAGG('TRIM(src.'|| IFF(src_c.array_path is not null,'value'|| IFF(src_column is not null,':'||TRIM(src_column),''),IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,'')))
    || ',\\\'"\\\') is null',' AND ')
  ||')' AS null_logic 
 FROM 
 (
   /*LINK Logic*/
    SELECT l.link_table_name AS table_name, lhr.hub_order_id, h_m.hub_column_order_id, h_m.version_id, h_m.src_table_id, h_m.src_column_id
   FROM MTD.mtd_link l
   JOIN MTD.mtd_link_hub_ref lhr 
   ON l.link_table_id = lhr.link_table_id
   AND l.version_id = lhr.version_id
   JOIN MTD.mtd_hub_map h_m
   ON lhr.hub_table_id = h_m.hub_table_id
   AND l.version_id = h_m.version_id
   
   UNION
   
   /*HUB Logic*/
 SELECT h.hub_table_name AS table_name, 1 , h_m.hub_column_order_id, h_m.version_id, h_m.src_table_id, h_m.src_column_id  
   FROM MTD.mtd_hub_map h_m
    JOIN MTD.mtd_hub h
    ON h.hub_table_id = h_m.hub_table_id
    AND h.version_id = h_m.version_id
   
   UNION
   
   /*SAT Logic*/
 SELECT s.sat_table_name AS table_name, sub_table.hub_order_id, sub_table.hub_column_order_id, sub_table.version_id, sub_table.src_table_id, sub_table.src_column_id
FROM MTD.mtd_sat s
JOIN(
  SELECT hub_table_id, -1 as link_table_id, 1 AS hub_order_id, h_m.hub_column_order_id, h_m.version_id, h_m.src_table_id, h_m.src_column_id  
   FROM MTD.mtd_hub_map h_m
 UNION
 SELECT -1 as hub_table_id, link_table_id, lhr.hub_order_id, h_m.hub_column_order_id, h_m.version_id, h_m.src_table_id, h_m.src_column_id
   FROM MTD.mtd_link_hub_ref lhr 
   JOIN MTD.mtd_hub_map h_m
   ON lhr.hub_table_id = h_m.hub_table_id
   AND lhr.version_id = h_m.version_id 
  ) sub_table
  ON (sub_table.hub_table_id = s.HUB_TABLE_ID OR sub_table.link_table_id = s.link_table_id)
  AND sub_table.version_id = s.version_id
   
 ) map
    JOIN MTD.MTD_SRC_COLUMN src_c
    ON  map.src_table_id = src_c.src_table_id
    AND map.src_column_id = src_c.src_column_id
    AND map.version_id = src_c.version_id
    
    JOIN MTD.MTD_SRC_TABLE src_t
    ON  map.src_table_id = src_t.src_table_id
    AND map.version_id = src_t.version_id

    GROUP BY  map.table_name, map.src_table_id, map.version_id;
