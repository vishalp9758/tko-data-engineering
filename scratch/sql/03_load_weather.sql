/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       03_load_weather.sql
Author:       Jeremiah Hansen
Last Updated: 1/2/2023
-----------------------------------------------------------------------------*/

USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;


-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- ----------------------------------------------------------------------------

/*---
But what about data that needs constant updating - like the WEATHER data? We would
need to build a pipeline process to constantly update that data to keep it fresh.

Perhaps a better way to get this external data would be to source it from a trusted
data supplier. Let them manage the data, keeping it accurate and up to date.

Enter the Snowflake Data Cloud...

Weather Source is a leading provider of global weather and climate data and their
OnPoint Product Suite provides businesses with the necessary weather and climate data
to quickly generate meaningful and actionable insights for a wide range of use cases
across industries. Let's connect to the "Weather Source LLC: frostbyte" feed from
Weather Source in the Snowflake Data Marketplace by following these steps:

    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Weather Source LLC: frostbyte" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "HOL_WEATHERSOURCE_DB" (all capital letters)
                        -> Grant to "HOL_ROLE"

We can then use this data as if it had been loaded directly into our account. And
we don't have to do any ETL work at all!    


-- You can also do it via code if you know the account/share details...
SET WEATHERSOURCE_ACCT_NAME = '*** PUT ACCOUNT NAME HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE_NAME = '*** PUT ACCOUNT SHARE HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE = $WEATHERSOURCE_ACCT_NAME || '.' || $WEATHERSOURCE_SHARE_NAME;

CREATE OR REPLACE DATABASE HOL_WEATHERSOURCE_DB
  FROM SHARE IDENTIFIER($WEATHERSOURCE_SHARE);

GRANT IMPORTED PRIVILEGES ON DATABASE HOL_WEATHERSOURCE_DB TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table
SELECT * FROM HOL_WEATHERSOURCE_DB.ONPOINT_ID.POSTAL_CODES LIMIT 100;
