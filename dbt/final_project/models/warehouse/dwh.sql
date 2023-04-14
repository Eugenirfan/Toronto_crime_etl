SELECT
   Offence_Number,
    REPORT_DATE,
    Occurence_date,
    REPORT_YEAR,
    REPORT_MONTH,
    REPORT_DAY,
    Report_Day_of_Year,
    Report_Day_of_Week,
    REPORT_HOUR,
    Occurence_year,
    Occurence_month,
    Occurence_day,
    Occurence_day_of_year,
    Occurence_day_of_week,
    OCC_HOUR,
    DIVISION,
    LOCATION_TYPE,
    PREMISES_TYPE,
    Offence_Code,
    Offence_Extension,
    OFFENCE,
    Offence_Category,
    NEIGHBOURHOOD_Identifier,
    NEIGHBOURHOOD,
    current_timestamp() AS Table_Created
FROM
    {{ ref('src_transformation')}}