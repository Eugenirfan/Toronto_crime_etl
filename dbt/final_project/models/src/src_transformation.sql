WITH raw_staging AS (
    SELECT * FROM   {{ source('src','crime_data')}}
)
SELECT
    EVENT_UNIQUE_ID AS Offence_Number,
    REPORT_DATE,
    OCC_DATE AS Occurence_date,
    REPORT_YEAR,
    REPORT_MONTH,
    REPORT_DAY,
    REPORT_DOY AS Report_Day_of_Year,
    REPORT_DOW AS Report_Day_of_Week,
    REPORT_HOUR,
    OCC_YEAR AS Occurence_year,
    OCC_MONTH AS Occurence_month,
    OCC_DAY AS Occurence_day,
    OCC_DOY AS Occurence_day_of_year,
    OCC_DOW AS Occurence_day_of_week,
    OCC_HOUR,
    DIVISION,
    LOCATION_TYPE,
    PREMISES_TYPE,
    UCR_CODE AS Offence_Code,
    UCR_EXT AS Offence_Extension,
    OFFENCE,
    MCI_CATEGORY AS Offence_Category,
    HOOD_158 AS NEIGHBOURHOOD_Identifier,
    NEIGHBOURHOOD_158 AS NEIGHBOURHOOD
FROM
    raw_staging