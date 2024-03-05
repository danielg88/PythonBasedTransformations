# PySpark-Python Time Based Transformations

This repo contains a series of Python Notebooks that I have used to make time based transformations with MicroStrategy using live connection datasets.

## 1. PySparkTimeTransformations

PySpark notebook with a function that returns a Dataframe with new columns for time based transformation. 

To use it:

From your dataframe identify the context columns (attributes), the columns with the numeric values that you want to transform (metrics) and the date column to use for the transformations.

Identify the transformations that you want to apply to the dataframe

Create a dataframe using the function applyDateTransformations with input parameters, your original dataframe, attributes, metrics, date column and required transformations.


```
    attributes = ['Region', 'Product', 'Date']
    metrics = ['Quantity','Price']
    date_column = 'Date'        
    transformations = ['MTD','MTD_PY']

    df_transformations = applyDateTransformations (df_data,attributes, metrics, date_column, transformations)
```

The final dataframe will contain new columns with the transformations results.


List of supported transformations:
- YTD: Year to date.
- YTD_PY: Year to date compared to the previous year.
- MTD: Month to date.
- MTD_PY: Month to date compared to the previous year.
- PD: Previous day.
- LD: Last december
- PM: Same day previous month.
- PM_LD: Last day of previous month.

## 2. PySparkTimeTable & PandasTimeTable
PySparkTimeTable creates a Date table using PySpark PandasTimeTable creates the same Datable table using Pandas.


This table has a series of auxiliary columns for live time based calculations in MicroStrategy.

join_date is used to join with the fact table

Year, closing_date, and month are used to filter in MicroStrategy.

Each metric in MicroStrategy must have a filter using any of the flags available.

### Table example

| closing_date |  join_date | year | cod_month | closing_date_rank   | year_rank | month_rank | flag_current_date   | flag_ytd | flag_yoy | flag_ytd_py   | flag_mom | flag_l12m | flag_l12m_py   |
|--------------|------------|------|-----------|---------------------|-----------|------------|---------------------|----------|----------|---------------|----------|-----------|----------------|
|   2023-12-31 | 31/01/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 28/02/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/03/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 30/04/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/05/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 30/06/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/07/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/08/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 30/09/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/10/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 30/11/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        0 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/12/2022 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        0 |        1 |             1 |        0 |         0 |              1 |
|   2023-12-31 | 31/01/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 28/02/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 31/03/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 30/04/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 31/05/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 30/06/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 31/07/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 31/08/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 30/09/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 31/10/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        0 |         1 |              0 |
|   2023-12-31 | 30/11/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   0 |        1 |        0 |             0 |        1 |         1 |              0 |
|   2023-12-31 | 31/12/2023 | 2023 |    202312 |                   1 |         1 |          1 |                   1 |        1 |        0 |             0 |        0 |         1 |              0 |