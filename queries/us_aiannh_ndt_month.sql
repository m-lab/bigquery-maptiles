#standardSQL
# AIANNH - us_american_indian_alaska_native_areas_hawaiian_home_lands
WITH aiannh AS (
  SELECT
    NAME AS name,
    WKT,
    GEOID
  FROM
    `measurement-lab.geographies.us_aiannh_2018`
),
state AS (
  SELECT 
    geo_id AS GEOID,
    state_name AS state_name,
    state_geom AS WKT
  FROM `bigquery-public-data.geo_us_boundaries.states`
),
dl AS (
  SELECT
    test_date,
    aiannh.GEOID AS GEOID,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    client.IP AS clientIP,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_downloads` tests, aiannh, state
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), aiannh.WKT
    )
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), state.WKT
    )
),
mlab_dl_perip_perday AS (
  SELECT
    test_date,
    GEOID,
    state,
    clientIP,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps,
    APPROX_QUANTILES(CAST(MinRTT AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt
  FROM dl
  GROUP BY test_date, GEOID, state, clientIP
),
aiannh_stats_dl AS (
  SELECT
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    GEOID,
    state,
    MIN(MIN_download_Mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS MAX_download_Mbps,
    APPROX_QUANTILES(CAST(MED_DL_min_rtt AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt
  FROM mlab_dl_perip_perday
  GROUP BY time_period, state, GEOID
),    
aiannh_dl_sample AS (
  SELECT 
    COUNT(*) AS aiannh_dl_sample_size,
    COUNT(DISTINCT clientIP) AS sample_dl_count_ips, 
    GEOID,
    state,
    FORMAT_DATE("%Y%m", test_date) AS time_period,
  FROM dl
  GROUP BY time_period, GEOID, state
),
ul AS (
  SELECT
    test_date,
    aiannh.GEOID AS GEOID,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    client.IP AS clientIP,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_uploads` tests, aiannh, state
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), aiannh.WKT
    )
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), state.WKT
    )
),
mlab_ul_perip_perday AS (
  SELECT
    test_date,
    GEOID,
    state,
    clientIP,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM ul
  GROUP BY test_date, GEOID, state, clientIP
),
aiannh_stats_ul AS (
  SELECT
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    GEOID,
    state, 
    MIN(MIN_upload_Mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS MAX_upload_Mbps
  FROM mlab_ul_perip_perday
  GROUP BY time_period, GEOID, state
),    
aiannh_ul_sample AS (
  SELECT 
    COUNT(*) AS aiannh_ul_sample_size,
    COUNT(DISTINCT clientIP) AS sample_ul_count_ips, 
    GEOID,
    state,
    FORMAT_DATE("%Y%m", test_date) AS time_period,
  FROM ul
  GROUP BY time_period, GEOID, state
),
DL_pct_levels AS (
  SELECT 
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    dl.GEOID,
    state,
    COUNTIF(mbps < 1) / COUNT(*) AS pct_under_1mbpsDL,
    COUNTIF(mbps < 4) / COUNT(*) AS pct_under_4mbpsDL,
    COUNTIF(mbps < 7) / COUNT(*) AS pct_under_7mbpsDL,
    COUNTIF(mbps < 10) / COUNT(*) AS pct_under_10mbpsDL,
    COUNTIF(mbps < 15) / COUNT(*) AS pct_under_15mbpsDL,
    COUNTIF(mbps < 25) / COUNT(*) AS pct_under_25mbpsDL,
    COUNTIF(mbps < 30) / COUNT(*) AS pct_under_30mbpsDL,
    COUNTIF(mbps < 50) / COUNT(*) AS pct_under_50mbpsDL, 
    COUNTIF(mbps < 100) / COUNT(*) AS pct_under_100mbpsDL,
    COUNTIF(mbps < 150) / COUNT(*) AS pct_under_150mbpsDL, 
    COUNTIF(mbps < 200) / COUNT(*) AS pct_under_200mbpsDL,
    COUNTIF(mbps < 300) / COUNT(*) AS pct_under_300mbpsDL, 
    COUNTIF(mbps < 400) / COUNT(*) AS pct_under_400mbpsDL,
    COUNTIF(mbps < 500) / COUNT(*) AS pct_under_500mbpsDL,
    COUNTIF(mbps < 600) / COUNT(*) AS pct_under_600mbpsDL,
    COUNTIF(mbps < 700) / COUNT(*) AS pct_under_700mbpsDL, 
    COUNTIF(mbps < 800) / COUNT(*) AS pct_under_800mbpsDL,
    COUNTIF(mbps < 900) / COUNT(*) AS pct_under_900mbpsDL, 
    COUNTIF(mbps < 1000) / COUNT(*) AS pct_under_1000mbpsDL,
    COUNTIF(mbps > 1) / COUNT(*) AS pct_over_1mbpsDL,
    COUNTIF(mbps > 4) / COUNT(*) AS pct_over_4mbpsDL,
    COUNTIF(mbps > 7) / COUNT(*) AS pct_over_7mbpsDL,
    COUNTIF(mbps > 10) / COUNT(*) AS pct_over_10mbpsDL,
    COUNTIF(mbps > 15) / COUNT(*) AS pct_over_15mbpsDL, 
    COUNTIF(mbps > 20) / COUNT(*) AS pct_over_20mbpsDL, 
    COUNTIF(mbps > 25) / COUNT(*) AS pct_over_25mbpsDL, 
    COUNTIF(mbps > 50) / COUNT(*) AS pct_over_50mbpsDL, 
    COUNTIF(mbps > 100) / COUNT(*) AS pct_over_100mbpsDL,
    COUNTIF(mbps > 200) / COUNT(*) AS pct_over_200mbpsDL, 
    COUNTIF(mbps > 300) / COUNT(*) AS pct_over_300mbpsDL,
    COUNTIF(mbps > 400) / COUNT(*) AS pct_over_400mbpsDL, 
    COUNTIF(mbps > 500) / COUNT(*) AS pct_over_500mbpsDL,
    COUNTIF(mbps > 600) / COUNT(*) AS pct_over_600mbpsDL, 
    COUNTIF(mbps > 700) / COUNT(*) AS pct_over_700mbpsDL,
    COUNTIF(mbps > 800) / COUNT(*) AS pct_over_800mbpsDL,
    COUNTIF(mbps > 900) / COUNT(*) AS pct_over_900mbpsDL,
    COUNTIF(mbps > 1000) / COUNT(*) AS pct_over_1000mbpsDL
  FROM dl
  GROUP BY time_period, GEOID, state
),
UL_pct_levels AS (
  SELECT 
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    ul.GEOID,
    state,
    COUNTIF(mbps < 1) / COUNT(*) AS pct_under_1mbpsUL,
    COUNTIF(mbps < 4) / COUNT(*) AS pct_under_4mbpsUL,
    COUNTIF(mbps < 7) / COUNT(*) AS pct_under_7mbpsUL,
    COUNTIF(mbps < 10) / COUNT(*) AS pct_under_10mbpsUL,
    COUNTIF(mbps < 15) / COUNT(*) AS pct_under_15mbpsUL,
    COUNTIF(mbps < 25) / COUNT(*) AS pct_under_25mbpsUL,
    COUNTIF(mbps < 30) / COUNT(*) AS pct_under_30mbpsUL,
    COUNTIF(mbps < 50) / COUNT(*) AS pct_under_50mbpsUL, 
    COUNTIF(mbps < 100) / COUNT(*) AS pct_under_100mbpsUL,
    COUNTIF(mbps < 150) / COUNT(*) AS pct_under_150mbpsUL, 
    COUNTIF(mbps < 200) / COUNT(*) AS pct_under_200mbpsUL,
    COUNTIF(mbps < 300) / COUNT(*) AS pct_under_300mbpsUL, 
    COUNTIF(mbps < 400) / COUNT(*) AS pct_under_400mbpsUL,
    COUNTIF(mbps < 500) / COUNT(*) AS pct_under_500mbpsUL,
    COUNTIF(mbps < 600) / COUNT(*) AS pct_under_600mbpsUL,
    COUNTIF(mbps < 700) / COUNT(*) AS pct_under_700mbpsUL, 
    COUNTIF(mbps < 800) / COUNT(*) AS pct_under_800mbpsUL,
    COUNTIF(mbps < 900) / COUNT(*) AS pct_under_900mbpsUL, 
    COUNTIF(mbps < 1000) / COUNT(*) AS pct_under_1000mbpsUL,
    COUNTIF(mbps > 1) / COUNT(*) AS pct_over_1mbpsUL,
    COUNTIF(mbps > 4) / COUNT(*) AS pct_over_4mbpsUL,
    COUNTIF(mbps > 7) / COUNT(*) AS pct_over_7mbpsUL,
    COUNTIF(mbps > 10) / COUNT(*) AS pct_over_10mbpsUL,
    COUNTIF(mbps > 15) / COUNT(*) AS pct_over_15mbpsUL, 
    COUNTIF(mbps > 20) / COUNT(*) AS pct_over_20mbpsUL, 
    COUNTIF(mbps > 25) / COUNT(*) AS pct_over_25mbpsUL, 
    COUNTIF(mbps > 50) / COUNT(*) AS pct_over_50mbpsUL, 
    COUNTIF(mbps > 100) / COUNT(*) AS pct_over_100mbpsUL,
    COUNTIF(mbps > 200) / COUNT(*) AS pct_over_200mbpsUL, 
    COUNTIF(mbps > 300) / COUNT(*) AS pct_over_300mbpsUL,
    COUNTIF(mbps > 400) / COUNT(*) AS pct_over_400mbpsUL, 
    COUNTIF(mbps > 500) / COUNT(*) AS pct_over_500mbpsUL,
    COUNTIF(mbps > 600) / COUNT(*) AS pct_over_600mbpsUL, 
    COUNTIF(mbps > 700) / COUNT(*) AS pct_over_700mbpsUL,
    COUNTIF(mbps > 800) / COUNT(*) AS pct_over_800mbpsUL,
    COUNTIF(mbps > 900) / COUNT(*) AS pct_over_900mbpsUL,
    COUNTIF(mbps > 1000) / COUNT(*) AS pct_over_1000mbpsUL
  FROM ul
  GROUP BY time_period, GEOID, state
)
SELECT * FROM aiannh_stats_dl
JOIN aiannh_stats_ul USING (time_period, GEOID, state)
JOIN aiannh_dl_sample USING (time_period, GEOID, state)
JOIN aiannh_ul_sample USING (time_period, GEOID, state)
JOIN DL_pct_levels USING (time_period, GEOID, state)
JOIN UL_pct_levels USING (time_period, GEOID, state)
JOIN aiannh USING (GEOID)