-- Calendar dimension table with configurable date range
-- Configuration keys: start_date (default: 2020-01-01), end_date (default: 2030-12-31)

CREATE OR REFRESH MATERIALIZED VIEW retail_q.retail_gold.dim_calendar
COMMENT 'Calendar dimension table with standard date attributes'
CLUSTER BY (date)
AS
SELECT
  date_col AS date,
  YEAR(date_col) AS year,
  MONTH(date_col) AS month,
  DAY(date_col) AS day,
  DAYOFWEEK(date_col) AS day_of_week,
  WEEKOFYEAR(date_col) AS week_of_year,
  QUARTER(date_col) AS quarter,
  DATE_FORMAT(date_col, 'EEEE') AS day_name,
  DATE_FORMAT(date_col, 'MMMM') AS month_name,
  DATE_FORMAT(date_col, 'yyyy-MM') AS year_month,
  CONCAT('Q', QUARTER(date_col), '-', YEAR(date_col)) AS year_quarter
FROM (
  SELECT EXPLODE(
    SEQUENCE(
      COALESCE(CAST('${start_date}' AS DATE), DATE'2020-01-01'),
      COALESCE(CAST('${end_date}' AS DATE), DATE'2030-12-31'),
      INTERVAL 1 DAY
    )
  ) AS date_col
)
