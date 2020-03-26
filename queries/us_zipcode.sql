#standardSQL
WITH dl AS (
  SELECT
    COUNT(a.UUID) AS ml_dl_count_tests,
    COUNT(DISTINCT client.IP) as ml_dl_count_ips,
    APPROX_QUANTILES(a.MeanThroughputMbps, 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_download_Mbps,
    APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_min_rtt,
    EXTRACT(YEAR FROM test_date) AS year,
    EXTRACT(MONTH FROM test_date) AS month,
    EXTRACT(WEEK FROM test_date) AS week,
    client.Geo.postal_code AS zip_code
  FROM
    `measurement-lab.library.ndt_unified_downloads` tests,
    `bigquery-public-data.geo_us_boundaries.zip_codes` zip_codes
  WHERE
    client.Geo.country_name = "United States"
    AND partition_date >= "2020-01-01"
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ),
      zip_codes.zcta_geom
    )
  GROUP BY
    zip_code,
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
    client.Geo.postal_code AS zip_code
  FROM
    `measurement-lab.release.ndt_uploads` tests,
    `bigquery-public-data.geo_us_boundaries.zip_codes` zip_codes
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND partition_date >= "2020-01-01"
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ),
      zip_codes.zcta_geom
    )
  GROUP BY
    zip_code,
    year,
    month,
    week
),
main AS (
  SELECT
    zip_code,
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
    JOIN ul USING (zip_code, year, month, week)
),
main_dec_2014 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2014,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2014,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2014,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2014,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2014,
    ml_download_Mbps AS ml_download_Mbps_dec_2014,
    ml_min_rtt AS ml_min_rtt_dec_2014
  FROM
    main
  WHERE
    time_period = 'dec_2014'
),
main_jun_2015 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2015,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2015,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2015,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2015,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2015,
    ml_download_Mbps AS ml_download_Mbps_jun_2015,
    ml_min_rtt AS ml_min_rtt_jun_2015
  FROM
    main
  WHERE
    time_period = 'jun_2015'
),
main_dec_2015 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2015,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2015,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2015,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2015,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2015,
    ml_download_Mbps AS ml_download_Mbps_dec_2015,
    ml_min_rtt AS ml_min_rtt_dec_2015
  FROM
    main
  WHERE
    time_period = 'dec_2015'
),
main_jun_2016 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2016,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2016,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2016,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2016,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2016,
    ml_download_Mbps AS ml_download_Mbps_jun_2016,
    ml_min_rtt AS ml_min_rtt_jun_2016
  FROM
    main
  WHERE
    time_period = 'jun_2016'
),
main_dec_2016 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2016,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2016,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2016,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2016,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2016,
    ml_download_Mbps AS ml_download_Mbps_dec_2016,
    ml_min_rtt AS ml_min_rtt_dec_2016
  FROM
    main
  WHERE
    time_period = 'dec_2016'
),
main_jun_2017 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2017,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2017,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2017,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2017,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2017,
    ml_download_Mbps AS ml_download_Mbps_jun_2017,
    ml_min_rtt AS ml_min_rtt_jun_2017
  FROM
    main
  WHERE
    time_period = 'jun_2017'
),
main_dec_2017 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2017,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2017,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2017,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2017,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2017,
    ml_download_Mbps AS ml_download_Mbps_dec_2017,
    ml_min_rtt AS ml_min_rtt_dec_2017
  FROM
    main
  WHERE
    time_period = 'dec_2017'
),
main_jun_2018 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2018,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2018,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2018,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2018,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2018,
    ml_download_Mbps AS ml_download_Mbps_jun_2018,
    ml_min_rtt AS ml_min_rtt_jun_2018
  FROM
    main
  WHERE
    time_period = 'jun_2018'
),
main_dec_2018 AS (
  SELECT
    zip_code,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2018,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2018,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2018,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2018,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2018,
    ml_download_Mbps AS ml_download_Mbps_dec_2018,
    ml_min_rtt AS ml_min_rtt_dec_2018
  FROM
    main
  WHERE
    time_period = 'dec_2018'
)
SELECT
  ml_ul_count_tests_dec_2014,
  ml_ul_count_ips_dec_2014,
  ml_upload_Mbps_dec_2014,
  ml_dl_count_tests_dec_2014,
  ml_dl_count_ips_dec_2014,
  ml_download_Mbps_dec_2014,
  ml_min_rtt_dec_2014,
  ml_ul_count_tests_jun_2015,
  ml_ul_count_ips_jun_2015,
  ml_upload_Mbps_jun_2015,
  ml_dl_count_tests_jun_2015,
  ml_dl_count_ips_jun_2015,
  ml_download_Mbps_jun_2015,
  ml_min_rtt_jun_2015,
  ml_ul_count_tests_dec_2015,
  ml_ul_count_ips_dec_2015,
  ml_upload_Mbps_dec_2015,
  ml_dl_count_tests_dec_2015,
  ml_dl_count_ips_dec_2015,
  ml_download_Mbps_dec_2015,
  ml_min_rtt_dec_2015,
  ml_ul_count_tests_jun_2016,
  ml_ul_count_ips_jun_2016,
  ml_upload_Mbps_jun_2016,
  ml_dl_count_tests_jun_2016,
  ml_dl_count_ips_jun_2016,
  ml_download_Mbps_jun_2016,
  ml_min_rtt_jun_2016,
  ml_ul_count_tests_dec_2016,
  ml_ul_count_ips_dec_2016,
  ml_upload_Mbps_dec_2016,
  ml_dl_count_tests_dec_2016,
  ml_dl_count_ips_dec_2016,
  ml_download_Mbps_dec_2016,
  ml_min_rtt_dec_2016,
  ml_ul_count_tests_jun_2017,
  ml_ul_count_ips_jun_2017,
  ml_upload_Mbps_jun_2017,
  ml_dl_count_tests_jun_2017,
  ml_dl_count_ips_jun_2017,
  ml_download_Mbps_jun_2017,
  ml_min_rtt_jun_2017,
  ml_ul_count_tests_dec_2017,
  ml_ul_count_ips_dec_2017,
  ml_upload_Mbps_dec_2017,
  ml_dl_count_tests_dec_2017,
  ml_dl_count_ips_dec_2017,
  ml_download_Mbps_dec_2017,
  ml_min_rtt_dec_2017,
  ml_ul_count_tests_jun_2018,
  ml_ul_count_ips_jun_2018,
  ml_upload_Mbps_jun_2018,
  ml_dl_count_tests_jun_2018,
  ml_dl_count_ips_jun_2018,
  ml_download_Mbps_jun_2018,
  ml_min_rtt_jun_2018,
  ml_ul_count_tests_dec_2018,
  ml_ul_count_ips_dec_2018,
  ml_upload_Mbps_dec_2018,
  ml_dl_count_tests_dec_2018,
  ml_dl_count_ips_dec_2018,
  ml_download_Mbps_dec_2018,
  ml_min_rtt_dec_2018,
  zip_codes.zip_code AS zip_code,
  zip_codes.county AS county_name,
  zip_codes.zcta_geom AS WKT
FROM
  `mlab-sandbox.usa_geo.us_zip_codes` zip_codes
  LEFT JOIN main_dec_2014 USING (zip_code)
  LEFT JOIN main_jun_2015 USING (zip_code)
  LEFT JOIN main_dec_2015 USING (zip_code)
  LEFT JOIN main_jun_2016 USING (zip_code)
  LEFT JOIN main_dec_2016 USING (zip_code)
  LEFT JOIN main_jun_2017 USING (zip_code)
  LEFT JOIN main_dec_2017 USING (zip_code)
  LEFT JOIN main_jun_2018 USING (zip_code)
  LEFT JOIN main_dec_2018 USING (zip_code);