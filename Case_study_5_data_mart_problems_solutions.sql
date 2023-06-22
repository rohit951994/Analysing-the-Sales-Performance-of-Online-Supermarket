
SET search_path = Case_study_5_data_mart;
--------------------------------------------------------------------1. Data Cleansing Steps------------------------------------------------------------
/*
In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:

Convert the week_date to a DATE format

Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc

Add a month_number with the calendar month for each week_date value as the 3rd column

Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values

Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value

segment	age_band
1	Young Adults
2	Middle Aged
3 or 4	Retirees
Add a new demographic column using the following mapping for the first letter in the segment values:
segment	demographic
C	Couples
F	Families
Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns

Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record  
*/
 SELECT * FROM weekly_sales

	DROP TABLE clean_weekly_sales 
	CREATE TABLE clean_weekly_sales AS
	SELECT to_date(week_date, 'DD-MM-YY') as week_date, EXTRACT(week FROM to_date(week_date, 'DD-MM-YY')) AS week_number,
		   EXTRACT(month FROM to_date(week_date, 'DD-MM-YY')) AS month_number,EXTRACT(year FROM to_date(week_date, 'DD-MM-YY')) AS year,
		   region,platform,
		   CASE WHEN segment='null' THEN 'Unknown' ELSE segment END AS segment,
		   CASE WHEN RIGHT(segment,1)='1' THEN 'Young Adults'
				WHEN RIGHT(segment,1)='2' THEN 'Middle Aged'
				WHEN RIGHT(segment,1) IN ('3','4') THEN 'Retirees'
				ELSE 'Unknown' END AS age_band ,
		   CASE WHEN LEFT(segment,1)='C' THEN 'Couples'
				WHEN LEFT(segment,1)='F' THEN 'Families'
				ELSE 'Unknown' END AS demographic,
			customer_type,transactions,sales,  ROUND((sales*1.0/transactions),2) AS avg_transactions	
	FROM weekly_sales

SELECT * FROM clean_weekly_sales

-----------------------------------------------------------------2. Data Exploration-------------------------------------------------------
--What day of the week is used for each week_date value?
	 SELECT DISTINCT  extract(dow from week_date ), to_char(week_date,'day')
	 FROM clean_weekly_sales
  
--What range of week numbers are missing from the dataset?
	WITH RECURSIVE week_count AS
		 ( SELECT 1 AS week_number
		 UNION ALL
		 SELECT week_number+1 
		 FROM week_count
		 WHERE week_number<52 )
	SELECT * FROM week_count
	WHERE week_number NOT IN (SELECT DISTINCT week_number FROM clean_weekly_sales)
  
--How many total transactions were there for each year in the dataset?
	SELECT year , SUM(transactions) total_transactions
	FROM clean_weekly_sales
	GROUP BY year

--What is the total sales for each region for each month?
	SELECT region,year,to_char(week_date,'month') as month ,SUM(sales)
	FROM clean_weekly_sales
	GROUP BY region,year,month
	ORDER BY region ,year, to_date(to_char(week_date,'month'),'month')

--What is the total count of transactions for each platform
    SELECT platform , SUM(transactions) count_of_transactions
    FROM clean_weekly_sales
    GROUP BY platform
--What is the percentage of sales for Retail vs Shopify for each month?
	WITH sales_based_on_platform AS
			(SELECT year ,month_number ,platform, sum (sales)
			FROM clean_weekly_sales
			GROUP BY year,month_number,platform
			ORDER BY year,month_number,platform),
		total_monthly_sales AS
			(SELECT year, month_number , sum(sales)
			FROM clean_weekly_sales
			GROUP BY year,month_number
			ORDER BY year,month_number)
	SELECT t1.year,t1.month_number,t1.platform,ROUND((t1.sum*100.0/t2.sum),2) as percent_of_sales
	FROM sales_based_on_platform t1
	JOIN total_monthly_sales t2 ON t1.year=t2.year AND t1.month_number=t2.month_number
--What is the percentage of sales by demographic for each year in the dataset?
WITH sales_based_on_demographic AS
			(SELECT year ,demographic ,sum (sales)
			FROM clean_weekly_sales
			GROUP BY year,demographic
			ORDER BY year,demographic),
	 total_monthly_sales AS
			(SELECT year , sum(sales)
			FROM clean_weekly_sales
			GROUP BY year
			ORDER BY year)
SELECT t1.year,t1.demographic,ROUND((t1.sum*100.0/t2.sum),2) as percent_of_sales
FROM sales_based_on_demographic t1
JOIN total_monthly_sales t2 ON t1.year=t2.year 

--Which age_band and demographic values contribute the most to Retail sales?
	 --Havent considered the unknown age_band and demographic
	 SELECT age_band,demographic ,sum(sales),ROUND((sum(sales)*100.0/(SELECT SUM(sales) FROM clean_weekly_sales)),2)
	 FROM clean_weekly_sales
	 WHERE platform='Retail' and age_band<>'Unknown'
	 GROUP BY age_band,demographic
	 ORDER BY sum(sales) desc
	 LIMIT 1
 
 
--Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?
  -- We cannot because we dont get correct result while we do average of an average .
	SELECT year , platform ,ROUND(sum(sales)/sum(transactions),2)
	FROM clean_weekly_sales
	GROUP BY year, platform
	ORDER BY year
	
	
-------------------------------------------------------3. Before & After Analysis----------------------------------------------------------------------

/*This technique is usually used when we inspect an important event and want to inspect the impact before and after a certain point in time.
Taking the week_date value of 2020-06-15 as the baseline week where the Data Mart sustainable packaging changes came into effect.
We would include all week_date values for 2020-06-15 as the start of the period after the change and the previous week_date values would be before

Using this analysis approach - answer the following questions:*/

--What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?
	SELECT * FROM clean_weekly_sales WHERE week_date='2020-06-15' -- To check the week in which changes happened . It is 25th week
	
WITH sale_4week_before_change AS 
		(SELECT SUM(sales) as sale_4_week_before
		FROM clean_weekly_sales
		WHERE week_number between 21 AND  24),
    sale_4week_after_change AS
		(SELECT SUM(sales) as sale_4_week_after
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 28)
SELECT sale_4_week_before,sale_4_week_after,ROUND((sale_4_week_after-sale_4_week_before)*100.0/sale_4_week_before,2) as growth_or_decline_in_sales
FROM sale_4week_before_change,sale_4week_after_change

--What about the entire 12 weeks before and after?
WITH sale_12week_before_change AS 
		(SELECT SUM(sales) as sale_12_week_before
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24),
    sale_12week_after_change AS
		(SELECT SUM(sales) as sale_12_week_after
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11)
SELECT sale_12_week_before,sale_12_week_after,ROUND((sale_12_week_after-sale_12_week_before)*100.0/sale_12_week_before,2) as growth_decline_in_sales
FROM sale_12week_before_change,sale_12week_after_change
	


--How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?

-- comparision of sales 12 week before and after change in 2020 with that of 2019 and 2020
WITH sale_12week_before_change AS 
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_before_change_2020
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
    sale_12week_after_change AS
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_after_change_2020
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
	sale_12week_before_change_2019 AS 
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_before_change_2019
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24 AND year=2019
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
	sale_12week_after_change_2019 AS 
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_after_change_2019
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11 AND year=2019
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
	sale_12week_before_change_2018 AS 
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_before_change_2018
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24 AND year=2018
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
	sale_12week_after_change_2018 AS 
		(SELECT region,platform,age_band,demographic,SUM(sales) as sale_12_week_after_change_2018
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11 AND year=2018
		GROUP BY region,platform,age_band,demographic
		ORDER BY region,platform,age_band,demographic),
    comparision_before_change AS
		(SELECT t1.region,t1.platform,t1.age_band,t1.demographic,t1.sale_12_week_before_change_2020,t2.sale_12_week_before_change_2019,t3.sale_12_week_before_change_2018,
		        ROUND(((t1.sale_12_week_before_change_2020-t3.sale_12_week_before_change_2018)*100.0/t3.sale_12_week_before_change_2018),2 )AS per_cent_change_in_sales_since_2018,
		        ROUND(((t1.sale_12_week_before_change_2020-t2.sale_12_week_before_change_2019)*100.0/t2.sale_12_week_before_change_2019),2 )AS per_cent_change_in_sales_since_2019 
		FROM sale_12week_before_change t1
		JOIN sale_12week_before_change_2019 t2 ON t1.region=t2.region AND t1.platform=t2.platform AND t1.age_band=t2.age_band AND t1.demographic=t2.demographic
		JOIN sale_12week_before_change_2018 t3 ON t1.region=t3.region AND t1.platform=t3.platform AND t1.age_band=t3.age_band AND t1.demographic=t3.demographic),
	comparision_after_change AS 
		(SELECT t1.region,t1.platform,t1.age_band,t1.demographic,t1.sale_12_week_after_change_2020,t2.sale_12_week_after_change_2019,t3.sale_12_week_after_change_2018,
		        ROUND(((t1.sale_12_week_after_change_2020-t3.sale_12_week_after_change_2018)*100.0/t3.sale_12_week_after_change_2018),2 )AS per_cent_change_in_sales_since_2018,
		        ROUND(((t1.sale_12_week_after_change_2020-t2.sale_12_week_after_change_2019)*100.0/t2.sale_12_week_after_change_2019),2 )AS per_Cent_change_in_sales_since_2019 
		FROM sale_12week_after_change t1
		JOIN sale_12week_after_change_2019 t2 ON t1.region=t2.region AND t1.platform=t2.platform AND t1.age_band=t2.age_band AND t1.demographic=t2.demographic
		JOIN sale_12week_after_change_2018 t3 ON t1.region=t3.region AND t1.platform=t3.platform AND t1.age_band=t3.age_band AND t1.demographic=t3.demographic)	

SELECT * FROM comparision_before_change
SELECT * FROM comparision_after_change

--sales 12 week before and after change in 2019
WITH sale_12week_before_change_2019 AS 
		(SELECT SUM(sales) as sale_12_week_before_change_2019
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24 AND year=2019),
	sale_12week_after_change_2019 AS 
		(SELECT SUM(sales) as sale_12_week_after_change_2019
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11 AND year=2019),
	sale_comparision_2019 AS
	    (SELECT t1.sale_12_week_before_change_2019,t2.sale_12_week_after_change_2019,
		 ROUND((t2.sale_12_week_after_change_2019-t1.sale_12_week_before_change_2019)*100.0/t1.sale_12_week_before_change_2019,2) as percent_change_before_and_after
        FROM sale_12week_before_change_2019 t1 ,sale_12week_after_change_2019 t2
		)
SELECT * FROM sale_comparision_2019

--sales 12 week before and after change in 2018
WITH sale_12week_before_change_2018 AS 
		(SELECT SUM(sales) as sale_12_week_before_change_2018
		FROM clean_weekly_sales
		WHERE week_number between 24-11 AND  24 AND year=2018
		),
	sale_12week_after_change_2018 AS 
		(SELECT SUM(sales) as sale_12_week_after_change_2018
		FROM clean_weekly_sales
		WHERE week_number between 25 AND 25+11 AND year=2018
		),
	sale_comparision_2018 AS
	    (SELECT t1.sale_12_week_before_change_2018,t2.sale_12_week_after_change_2018,
		 ROUND((t2.sale_12_week_after_change_2018-t1.sale_12_week_before_change_2018)*100.0/t1.sale_12_week_before_change_2018,2) as percent_change_before_and_after
        FROM sale_12week_before_change_2018 t1,sale_12week_after_change_2018 t2
		)
SELECT * FROM sale_comparision_2018	


-----------------------------------------------------------------------------4. Bonus Question----------------------------------------------------------------------
--Which areas of the business have the highest negative impact in sales metrics performance in 2020 for the 12 week before and after period?
SELECT * FROM clean_weekly_sales

--Answer
--On comparing for REGION there has been more than 3% decline in ASIA AND OCEANIA , Almost 2% in CANADA and SOUNTH AMERICA .WHILE a 4.73 % rise in EUROPE .
--Retail sales declined by 2.43% while Shopify sales increased by 7.18% .
--There have been decline in sales across all age band Middle Aged(1.97%),Retiree(1.23%),Unknown(3.34%) .Least is for young adults(.92%)
--Sales decline for couples(.87%) , Families(1.82%) , Unknow(3.34%)
--Sales from New customers increased by 1.01% while decrease for existing(2.27%) and Guest(3%)


--Change based on region
	WITH week_12_sales_region_before_change AS 
			(SELECT region,SUM(sales) as sale_12_week_before_change
			FROM clean_weekly_sales
			WHERE week_number between 24-11 AND  24 AND year=2020
			GROUP BY region
			ORDER BY region),
		week_12_sales_region_after_change AS 
			(SELECT region,SUM(sales) as sale_12_week_after_change
			FROM clean_weekly_sales
			WHERE week_number between 25 AND 25+11 AND year=2020
			GROUP BY region
			ORDER BY region)
	SELECT t1.region,t1.sale_12_week_before_change , t2.sale_12_week_after_change,
		   round((t2.sale_12_week_after_change-t1.sale_12_week_before_change)*100.0/t1.sale_12_week_before_change,2) percent_change
	FROM week_12_sales_region_before_change t1 
	JOIN week_12_sales_region_after_change t2 ON t1.region=t2.region

--Change based on platform
	WITH week_12_sales_platform_before_change AS 
			(SELECT platform,SUM(sales) as sale_12_week_before_change
			FROM clean_weekly_sales
			WHERE week_number between 24-11 AND  24 AND year=2020
			GROUP BY platform
			ORDER BY platform),
		week_12_sales_platform_after_change AS 
			(SELECT platform,SUM(sales) as sale_12_week_after_change
			FROM clean_weekly_sales
			WHERE week_number between 25 AND 25+11 AND year=2020
			GROUP BY platform
			ORDER BY platform)
	SELECT t1.platform,t1.sale_12_week_before_change , t2.sale_12_week_after_change,
		   round((t2.sale_12_week_after_change-t1.sale_12_week_before_change)*100.0/t1.sale_12_week_before_change,2) percent_change
	FROM week_12_sales_platform_before_change t1 
	JOIN week_12_sales_platform_after_change t2 ON t1.platform=t2.platform

--Change based on age band
	WITH week_12_sales_age_band_before_change AS 
			(SELECT age_band,SUM(sales) as sale_12_week_before_change
			FROM clean_weekly_sales
			WHERE week_number between 24-11 AND  24 AND year=2020
			GROUP BY age_band
			ORDER BY age_band),
		week_12_sales_age_band_after_change AS 
			(SELECT age_band,SUM(sales) as sale_12_week_after_change
			FROM clean_weekly_sales
			WHERE week_number between 25 AND 25+11 AND year=2020
			GROUP BY age_band
			ORDER BY age_band)
	SELECT t1.age_band,t1.sale_12_week_before_change , t2.sale_12_week_after_change,
		   round((t2.sale_12_week_after_change-t1.sale_12_week_before_change)*100.0/t1.sale_12_week_before_change,2) percent_change
	FROM week_12_sales_age_band_before_change t1 
	JOIN week_12_sales_age_band_after_change t2 ON t1.age_band=t2.age_band

--Change based on demographic
	WITH week_12_sales_demographic_before_change AS 
			(SELECT demographic,SUM(sales) as sale_12_week_before_change
			FROM clean_weekly_sales
			WHERE week_number between 24-11 AND  24 AND year=2020
			GROUP BY demographic
			ORDER BY demographic),
		week_12_sales_demographic_after_change AS 
			(SELECT demographic,SUM(sales) as sale_12_week_after_change
			FROM clean_weekly_sales
			WHERE week_number between 25 AND 25+11 AND year=2020
			GROUP BY demographic
			ORDER BY demographic)
	SELECT t1.demographic,t1.sale_12_week_before_change , t2.sale_12_week_after_change,
		   round((t2.sale_12_week_after_change-t1.sale_12_week_before_change)*100.0/t1.sale_12_week_before_change,2) percent_change
	FROM week_12_sales_demographic_before_change t1 
	JOIN week_12_sales_demographic_after_change t2 ON t1.demographic=t2.demographic


--Change based on customer_type
	WITH week_12_sales_customer_type_before_change AS 
			(SELECT customer_type,SUM(sales) as sale_12_week_before_change
			FROM clean_weekly_sales
			WHERE week_number between 24-11 AND  24 AND year=2020
			GROUP BY customer_type
			ORDER BY customer_type),
		week_12_sales_customer_type_after_change AS 
			(SELECT customer_type,SUM(sales) as sale_12_week_after_change
			FROM clean_weekly_sales
			WHERE week_number between 25 AND 25+11 AND year=2020
			GROUP BY customer_type
			ORDER BY customer_type)
	SELECT t1.customer_type,t1.sale_12_week_before_change , t2.sale_12_week_after_change,
		   round((t2.sale_12_week_after_change-t1.sale_12_week_before_change)*100.0/t1.sale_12_week_before_change,2) percent_change
	FROM week_12_sales_customer_type_before_change t1 
	JOIN week_12_sales_customer_type_after_change t2 ON t1.customer_type=t2.customer_type
		