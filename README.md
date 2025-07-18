**IBM DATA PIPELINE**

**Project Overview:** 
This project is an end–to–end scalable data pipeline.

**Features:**
- Data Ingestion: Extracted raw data from CSV files stored in IBM Cloud Object Storage.
- Data Transformation: Cleaned and formatted the data using Db2 SQL queries on the IBM Data Engine platform, and staged the cleaned files for structured loading.
- Data Loading: Loaded transformed data into IBM Db2 tables using Python and JDBC (jaydebeapi), optimizing performance with SYSPROC.ADMIN CMD.
- Change Data Capture (CDC): Set up a process for efficient record updates, scheduled daily runs via cron jobs, and integrated logging for monitoring and debugging.

**Tech Stack:** IBM Cloud, IBM Db2, IBM Data Engine, IBM COS SDK, Db2 SQL, Python, JDBC (for connecting to Db2)
