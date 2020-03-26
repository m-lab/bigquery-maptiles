#standardSQL
WITH zipcodes AS (
  SELECT * FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
),
dl AS (
  SELECT
    COUNT(a.UUID) AS ml_dl_count_tests,
    COUNT(DISTINCT client.IP) as ml_dl_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps, 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_download_Mbps,
    APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_min_rtt,
    EXTRACT(YEAR FROM test_date) AS year,
    EXTRACT(MONTH FROM test_date) AS month,
    EXTRACT(WEEK FROM test_date) AS week,
    zip_codes.zip_code AS zip
  FROM
    `measurement-lab.library.ndt_unified_downloads` tests,
    `bigquery-public-data.geo_us_boundaries.zip_codes` zip_codes
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= "2020-01-01"
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ),
      zip_codes.zip_code_geom
    )
  GROUP BY
    zip,
    year,
    month,
    week
),
ul AS (
  SELECT
    COUNT(a.UUID) AS ml_ul_count_tests,
    COUNT(DISTINCT client.IP) AS ml_ul_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps, 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_upload_Mbps,
    EXTRACT(YEAR FROM test_date) AS year,
    EXTRACT(MONTH FROM test_date) AS month,
    EXTRACT(WEEK FROM test_date) AS week,
    zip_codes.zip_code AS zip
  FROM
    `mlab-sandbox.library.ndt_unified_uploads` tests,
    `bigquery-public-data.geo_us_boundaries.zip_codes` zip_codes
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= "2020-01-01"
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ),
      zip_codes.zip_code_geom
    )
  GROUP BY
    zip,
    year,
    month,
    week
)
SELECT
  zip,
  year,
  month,
  week,
  ml_ul_count_tests,
  ml_ul_count_ips,
  ml_upload_Mbps,
  ml_dl_count_tests,
  ml_dl_count_ips,
  ml_download_Mbps,
  ml_min_rtt
FROM
  dl
  JOIN ul USING (zip, year, month, week)
  JOIN zipcodes ON (dl.zip = zipcodes.zip_code)
