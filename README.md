# PySpark in Databricks

This notebook uses PySpark session in Databricks to accomplish various tasks related to Call Detail Record (CDR) data analysis.

1. About CDR Data
CDR is the name of Call Detail Record. The data is located on WeCloudData`s S3 bucket in the CDR folder. Each month has a sub-folder, and each day has a single file. Each file has the same columns, which are:

- Square id
- Time interval
- Country code
- SMS-in activity
- SMS-out activity
- Call-in activity
- Call-out activity
- Internet traffic activity

2. Project Requirement
The entire project needs to be finished in Databricks. The following tasks need to be accomplished:

- Mount CDR data from S3 bucket to Databricks `hdfs` files system`s `/mnt/` folder. Use the code provided in the notebook to mount the data.
- Read only the sub-folder `cdr_by_grid_december` in the CDR folder.
- Use top 5 days data for analysis, which means read 5 files from the sub-folder.

3. Tasks
The notebook accomplishes the following tasks:

- Change the column names by replacing `-` with `_`.
- Add a new column named `sms_ratio` showing the ratio of `sms-in-activity/sms-out-activity`.
- Create a date column by changing the `time_interval` column to timestamp first and then changing the date format to `yyyy/MM/dd`.
- Calculate summaray statistics at the square_id level by creating a dataframe that calculates the aggregation of `sms_in_activity`, `sms_out_activity`, `call_out_activity`, `internet_activity` and all records count.
- Find the min and max by grouping by `square_id` and finding out the min and max value in columns `sms_in_activity`, `sms_out_activity`, `internet_activity`, `call_in_activity`, `call_out_activity`.
- Create a summary table by generating an aggregate table to summarize how many SMS, call, and internet activities are in each country each day. Write the dataframe to the `tmp` folder in parquet format.
- Create a dataframe rank internet activity with Window function by using window function, partition by `country_code`, rank the total internet activities of each day.
