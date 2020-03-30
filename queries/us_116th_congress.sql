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
    APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(51)] AS MED_download_Mbps,
    APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 101) [ORDINAL(51)] as MED_DL_min_rtt,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS time_period,
    districts.geo_id AS GEOID
  FROM
    `measurement-lab.library.ndt_unified_downloads` tests,
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
    APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(51)] AS MED_upload_Mbps,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS time_period,
    districts.geo_id AS GEOID
  FROM
    `mlab-sandbox.library.ndt_unified_uploads` tests,
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
    GEOID, time_period, dl_count_tests, dl_count_ips, MED_download_Mbps, MED_DL_min_rtt,
    ul_count_tests, ul_count_ips, MED_upload_Mbps
  FROM
  mlab_dl JOIN mlab_ul USING (GEOID, time_period)
),
main_2020w1 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w1,
    dl_count_ips AS dl_count_ips_2020w1,
    MED_download_Mbps AS MED_download_Mbps_2020w1,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w1,
    ul_count_tests AS ul_count_tests_2020w1, 
    ul_count_ips AS ul_count_ips_2020w1, 
    MED_upload_Mbps AS MED_upload_Mbps_2020w1
  FROM main
  WHERE time_period = "2020-1-1"  
),
main_2020w2 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w2,
    dl_count_ips AS dl_count_ips_2020w2,
    MED_download_Mbps AS MED_download_Mbps_2020w2,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w2,
    ul_count_tests AS ul_count_tests_2020w2, 
    ul_count_ips AS ul_count_ips_2020w2, 
    MED_upload_Mbps AS MED_upload_Mbps_2020w2
  FROM main
  WHERE time_period = "2020-1-2"  
),
main_2020w3 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w3,
    dl_count_ips AS dl_count_ips_2020w3,
    MED_download_Mbps AS MED_download_Mbps_2020w3,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w3,
    ul_count_tests AS ul_count_tests_2020w3, 
    ul_count_ips AS ul_count_ips_2020w3,
    MED_upload_Mbps AS MED_upload_Mbps_2020w3
  FROM main
  WHERE time_period = "2020-1-3"  
),
main_2020w4 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w4,
    dl_count_ips AS dl_count_ips_2020w4,
    MED_download_Mbps AS MED_download_Mbps_2020w4,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w4,
    ul_count_tests AS ul_count_tests_2020w4, 
    ul_count_ips AS ul_count_ips_2020w4,
    MED_upload_Mbps AS MED_upload_Mbps_2020w4
  FROM main
  WHERE time_period = "2020-1-4"
),
main_2020w5 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w5,
    dl_count_ips AS dl_count_ips_2020w5,
    MED_download_Mbps AS MED_download_Mbps_2020w5,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w5,
    ul_count_tests AS ul_count_tests_2020w5, 
    ul_count_ips AS ul_count_ips_2020w5,
    MED_upload_Mbps AS MED_upload_Mbps_2020w5
  FROM main
  WHERE time_period = "2020-1-5"
),
main_2020w6 AS (
  SELECT GEOID, time_period, 
    dl_count_tests AS dl_count_tests_2020w6,
    dl_count_ips AS dl_count_ips_2020w6,
    MED_download_Mbps AS MED_download_Mbps_2020w6,
    MED_DL_min_rtt AS MED_DL_min_rtt_2020w6,
    ul_count_tests AS ul_count_tests_2020w6, 
    ul_count_ips AS ul_count_ips_2020w6,
    MED_upload_Mbps AS MED_upload_Mbps_2020w6
  FROM main
  WHERE time_period = "2020-2-6"
)
SELECT
    dl_count_tests_2020w1,
    dl_count_ips_2020w1,
    MED_download_Mbps_2020w1,
    MED_DL_min_rtt_2020w1,
    ul_count_tests_2020w1, 
    ul_count_ips_2020w1, 
    MED_upload_Mbps_2020w1,
    dl_count_tests_2020w2,
    dl_count_ips_2020w2,
    MED_download_Mbps_2020w2,
    MED_DL_min_rtt_2020w2,
    ul_count_tests_2020w2, 
    ul_count_ips_2020w2, 
    MED_upload_Mbps_2020w2,
    dl_count_tests_2020w3,
    dl_count_ips_2020w3,
    MED_download_Mbps_2020w3,
    MED_DL_min_rtt_2020w3,
    ul_count_tests_2020w3, 
    ul_count_ips_2020w3,
    MED_upload_Mbps_2020w3,
    dl_count_tests_2020w4,
    dl_count_ips_2020w4,
    MED_download_Mbps_2020w4,
    MED_DL_min_rtt_2020w4,
    ul_count_tests_2020w4, 
    ul_count_ips_2020w4,
    MED_upload_Mbps_2020w4,
    dl_count_tests_2020w5,
    dl_count_ips_2020w5,
    MED_download_Mbps_2020w5,
    MED_DL_min_rtt_2020w5,
    ul_count_tests_2020w5, 
    ul_count_ips_2020w5,
    MED_upload_Mbps_2020w5,
    dl_count_tests_2020w6,
    dl_count_ips_2020w6,
    MED_download_Mbps_2020w6,
    MED_DL_min_rtt_2020w6,
    ul_count_tests_2020w6, 
    ul_count_ips_2020w6,
    MED_upload_Mbps_2020w6,
    districts.geo_id as GEOID,
    districts.lsad_name AS name,
    districts.district_geom AS WKT
FROM
  `bigquery-public-data.geo_us_boundaries.congress_district_116` districts
  LEFT JOIN main_2020w1 ON (districts.geo_id = GEOID)
  LEFT JOIN main_2020w2 USING (GEOID)
  LEFT JOIN main_2020w3 USING (GEOID)
  LEFT JOIN main_2020w4 USING (GEOID)
  LEFT JOIN main_2020w5 USING (GEOID)
  LEFT JOIN main_2020w6 USING (GEOID);