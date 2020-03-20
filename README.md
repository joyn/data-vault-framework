# Snowflake DataVault 2.0 Framework

This project holds Snowflake Metadata Tables and Snowflake Stored Procedures to Create Data Vault 2.0 Tables, Snowflake [Streams](https://docs.snowflake.net/manuals/sql-reference/sql/create-stream.html) and [Tasks](https://docs.snowflake.net/manuals/sql-reference/sql/create-task.html) for the Loading of the Tables.
The Framework can handle Plain Table, VARIANT Columns (JSON, AVRO), Arrays, Schema on Read and explicit definition of each Field.


##Structure

Tables:
* MTD.MTD_SRC_TABLE
* MTD.MTD_SRC_COLUMN
* MTD.MTD_HUB
* MTD.MTD_HUB_MAP
* MTD.MTD_LINK
* MTD.MTD_LINK_HUB_REF
* MTD.MTD_LINK_MAP
* MTD.MTD_SAT 
* MTD.MTD_SAT_MAP
* MTD.MTD_MODEL_CREATION_LOG
* MTD.ETL_SQL

Views:
* MTD.MTD_HASH_KEY

Stored Porcedures:
* MTD.PROC_CREATE_TABLES
* MTD.PROC_ETL_HUB
* MTD.PROC_ETL_LINK
* MTD.PROC_ETL_SAT
* UTIL.PROC_ETL_STREAM_TASK_CREATE

###Table Desciption

MTD.MTD_SRC_TABLE
This table contains the Source Definition and Path to the Staging Table.

MTD.MTD_SRC_COLUMN
The Definition and Path to each Column is defined in The SRC_COLUMN Table, here you can define the Array Path aswell and add some Filters.

MTD.MTD_HUB
Definition of the HUB Table Name.
MTD.MTD_HUB_MAP
Field Mappings of the Business Keys from SRC_TABLE & SRC_COLUMN and Definition of Order.
MTD.MTD_LINK
Definition of the LINK Table Name and if its a NonHistorized Link
MTD.MTD_LINK_HUB_REF
Definition of LINK - HUB Relations
MTD.MTD_LINK_MAP
Field Mappings of the Values from SRC_TABLE & SRC_COLUMN to a Non Historized Link (only used if NonHist-Flag is true in MTD_LINK)
MTD.MTD_SAT 
Definition of the SAT Table Name, Reference to the Parrent Object (HUB/LINK), if a Hash Diff is necessary and teh possibility to define as Multi Active Satelite
MTD.MTD_SAT_MAP
Field Mappings of the Values from SRC_TABLE & SRC_COLUMN for the Satelite Table
MTD.MTD_MODEL_CREATION_LOG
Log Table, where you can activate or deactivate a deployment/version
MTD.ETL_SQL
Table where all ETL Scripts are stored before they get into the TASK. It's the base for TASK & STREAM Creation