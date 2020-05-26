#standardSQL
WITH congress_district_116 AS (
  SELECT
    geo_id,
    state_fips_code,
    district_fips_code,
    lsad_name AS district_lsad_name,
    lsad_code AS district_lsad_code,
    int_point_lat,
    int_point_lon,
    district_geom AS WKT,
  FROM
    `bigquery-public-data.geo_us_boundaries.congress_district_116`
),
mlab_dl AS (
  SELECT
    COUNT(a.UUID) AS dl_count_tests,
    COUNT(DISTINCT client.IP) AS dl_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(a.MeanThroughputMbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(a.MeanThroughputMbps) AS MAX_download_Mbps,
    APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date)) AS time_period,
    districts.geo_id AS GEOID
  FROM
    `measurement-lab.ndt.unified_downloads` tests,
    `bigquery-public-data.geo_us_boundaries.congress_district_116` districts
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), district_geom
    )
    GROUP BY GEOID, time_period
),
mlab_ul AS (
  SELECT
    COUNT(a.UUID) AS ul_count_tests,
    COUNT(DISTINCT client.IP) AS ul_count_ips,
    MIN(a.MeanThroughputMbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(a.MeanThroughputMbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(a.MeanThroughputMbps) AS MAX_upload_Mbps,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date)) AS time_period,
    districts.geo_id AS GEOID
  FROM
    `measurement-lab.ndt.unified_uploads` tests,
    `bigquery-public-data.geo_us_boundaries.congress_district_116` districts
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), district_geom
    )
  GROUP BY GEOID, time_period
),
main AS (
  SELECT
    GEOID, time_period, dl_count_tests, dl_count_ips, MIN_download_Mbps, 
    LOWER_QUART_download_Mbps, MED_download_Mbps, MEAN_download_Mbps, UPPER_QUART_download_Mbps,
    MAX_download_Mbps, MED_DL_min_rtt, ul_count_tests, ul_count_ips, MIN_upload_Mbps, 
    LOWER_QUART_upload_Mbps, MED_upload_Mbps, MEAN_upload_Mbps, UPPER_QUART_upload_Mbps,
    MAX_upload_Mbps
  FROM
  mlab_dl JOIN mlab_ul USING (GEOID, time_period)
),
month_agg AS (
  WITH months AS (
    SELECT DISTINCT(time_period) FROM main
  )
  SELECT 
    GEOID, main.time_period, dl_count_tests, dl_count_ips, MIN_download_Mbps, 
    LOWER_QUART_download_Mbps, MED_download_Mbps, MEAN_download_Mbps, UPPER_QUART_download_Mbps,
    MAX_download_Mbps, MED_DL_min_rtt, ul_count_tests, ul_count_ips, MIN_upload_Mbps, 
    LOWER_QUART_upload_Mbps, MED_upload_Mbps, MEAN_upload_Mbps, UPPER_QUART_upload_Mbps,
    MAX_upload_Mbps
  FROM main, months
  WHERE main.time_period = months.time_period
)
SELECT * FROM month_agg
JOIN congress_district_116 ON GEOID = congress_district_116.geo_id
