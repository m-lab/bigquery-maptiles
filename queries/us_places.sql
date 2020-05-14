#standardSQL
WITH us_places AS (
  SELECT * FROM `mlab-sandbox.usa_geo.us_places`
),
mlab_dl AS (
  SELECT
    COUNT(a.UUID) AS dl_count_tests,
    COUNT(DISTINCT client.IP) AS dl_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS time_period,
    us_places.GEOID AS geoid
  FROM
    `measurement-lab.ndt.unified_downloads` tests, us_places
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), us_places.WKT
    )
    GROUP BY geoid, time_period
),
mlab_ul AS (
  SELECT
    COUNT(a.UUID) AS ul_count_tests,
    COUNT(DISTINCT client.IP) AS ul_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps,100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS time_period,
    us_places.GEOID AS geoid
  FROM
    `measurement-lab.ndt.unified_uploads` tests, us_places
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), us_places.WKT
    )
  GROUP BY geoid, time_period
),
main AS (
  SELECT
    geoid, time_period, dl_count_tests, dl_count_ips, MED_download_Mbps, MED_DL_min_rtt,
    ul_count_tests, ul_count_ips, MED_upload_Mbps
  FROM
  mlab_dl JOIN mlab_ul USING (geoid, time_period)
),
week_agg AS (
  WITH weeks AS (
    SELECT DISTINCT(time_period) FROM main
  )
  SELECT 
    geoid AS geo_id, main.time_period, dl_count_tests, dl_count_ips, MED_download_Mbps, MED_DL_min_rtt,
    ul_count_tests, ul_count_ips, MED_upload_Mbps
  FROM main, weeks
  WHERE main.time_period = weeks.time_period
)
SELECT * FROM week_agg
JOIN us_places ON week_agg.geo_id = us_places.GEOID
ORDER BY week_agg.geo_id, time_period
