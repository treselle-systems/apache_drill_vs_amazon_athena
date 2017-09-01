
/*To alter session as parquet*/

1. ALTER SESSION SET `store.format`='parquet';

/*Create table using csv file with partition by gender in drill*/

2.CREATE TABLE dfs.`csvOut`.AGE_FERTILITY_RATES_GENDER_PARQUET_PARTITION(country_code, country_name, `year`, fertility_rate_15_19, fertility_rate_20_24, fertility_rate_25_29, fertility_rate_30_34, fertility_rate_35_39, fertility_rate_40_44, fertility_rate_45_49, total_fertility_rate, gross_reproduction_rate, sex_ratio_at_birth,gender) 
PARTITION BY (gender)
AS
SELECT columns[0] as country_code,columns[1] as country_name,columns[2] as `year`,columns[3] as fertility_rate_15_19,columns[4] as fertility_rate_20_24,columns[5] as fertility_rate_25_29,columns[6] as fertility_rate_30_34,columns[7] as fertility_rate_35_39,columns[8] as fertility_rate_40_44,columns[9] as fertility_rate_45_49,columns[10] as total_fertility_rate,columns[11] as gross_reproduction_rate,columns[12] as sex_ratio_at_birth,columns[13] as gender FROM dfs.`/user/tsldp/drillathena/age_specific_fertility_rates_gender.csv`;

/*To retrieve the data from partition file*/

3.select * from dfs.`csvOut`.`AGE_FERTILITY_RATES_GENDER_PARQUET_PARTITION` ;

/* To retrieve the count for both male and female respectively*/

4. select count(*),gender from dfs.`csvOut`.`AGE_FERTILITY_RATES_GENDER_PARQUET_PARTITION` group by gender;

/*create external table using parquet file format with partition by gender in athena*/

5.create external table sampledb.age_fertility_rates_gender_parq_part(
country_code string,
country_name string,
year string,
fertility_rate_15_19 decimal(10,5),
fertility_rate_20_24 decimal(10,5),
fertility_rate_25_29 decimal(10,5),
fertility_rate_30_34 decimal(10,5),
fertility_rate_35_39 decimal(10,5),
fertility_rate_40_44 decimal(10,5),
fertility_rate_45_49 decimal(10,5),
total_fertility_rate decimal(10,5),
gross_reproduction_rate decimal(10,5),
sex_ratio_at_birth decimal(10,5))
PARTITIONED BY (gender string)
stored as parquet
LOCATION 's3://cps3bucket/data_gender_parquet/';

/* To view the partition stored in metadata*/

6.MSCK REPAIR TABLE age_fertility.age_fertility_rates_gender_parq_part;

/* To retrieve the data in partition table*/

7.select * from age_fertility.age_fertility_rates_gender_parq_part;
