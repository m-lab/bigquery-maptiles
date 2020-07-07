#standardSQL
WITH counties AS (
  SELECT
    county_name,
    county_geom AS WKT,
    geo_id AS GEOID
  FROM
    `bigquery-public-data.geo_us_boundaries.counties`
),
state AS (
  SELECT 
    state_name AS state,
    state_geom AS WKT
  FROM `bigquery-public-data.geo_us_boundaries.states`
),
dl_per_location_cleaned AS (
  SELECT
    test_date,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    counties.GEOID AS GEOID,
    counties.county_name AS county_name,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
  FROM
    `measurement-lab.ndt.unified_downloads` tests, counties, state
  WHERE
    client.Geo.country_name = "United States"
    AND client.Geo.region = "NC"
    AND test_date = @startday
    AND client.Geo.country_code IS NOT NULL 
    AND client.Geo.country_code != "" 
    AND client.Geo.region IS NOT NULL 
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), counties.WKT
    )
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), state.WKT
    )
),
dl_stats_perip_perday AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    ip,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name, ip
),
# Final stats per day
dl_stats_per_day AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name,
    MIN(MIN_download_Mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS MAX_download_Mbps
  FROM
    dl_stats_perip_perday
  GROUP BY test_date, state, GEOID, county_name
),
dl_total_samples_per_geo AS (
  SELECT
    test_date,
    COUNT(*) AS dl_total_samples,
    state,
    GEOID,
    county_name,
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name
),
# Now begin generating histograms of Max DL
max_dl_per_day_ip AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    ip,
    MAX(mbps) AS mbps
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name, ip
),
# Count the samples
dl_sample_counts AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    test_date,
    state,
    GEOID,
    county_name
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10,x) AS bucket_right, POW(10, x-.2) AS bucket_left
  FROM UNNEST(GENERATE_ARRAY(0, 3, .2)) AS x
),
dl_histogram_counts AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    state,
    GEOID,
    county_name,
    bucket_min,
    bucket_max
  ORDER BY 
    test_date,
    state,
    GEOID,
    county_name,
    bucket_min,
    bucket_max
),
dl_histogram AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name, 
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts 
  JOIN dl_sample_counts USING (test_date, state, GEOID, county_name)
),
ul_per_location_cleaned AS (
  SELECT
    test_date,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    counties.GEOID AS GEOID,
    counties.county_name AS county_name,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
  FROM
    `measurement-lab.ndt.unified_uploads` tests, counties, state
  WHERE
    client.Geo.country_name = "United States"
    AND client.Geo.region = "NC"
    AND test_date = @startday
    AND client.Geo.country_code IS NOT NULL 
    AND client.Geo.country_code != "" 
    AND client.Geo.region IS NOT NULL 
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), counties.WKT
    )
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), state.WKT
    )
),
max_ul_per_day_ip AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    ip,
    MAX(mbps) AS mbps
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name, ip
),
# Count the samples
ul_sample_counts AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    test_date,
    state,
    GEOID,
    county_name
),
ul_histogram_counts AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    state,
    GEOID,
    county_name,
    bucket_min,
    bucket_max
  ORDER BY 
    test_date,
    state,
    GEOID,
    county_name,
    bucket_min,
    bucket_max
),
ul_histogram AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name, 
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts 
  JOIN ul_sample_counts USING (test_date, state, GEOID, county_name)
),
# Generate upload test stats for each geo per ip, per day
ul_stats_perip_perday AS (
  SELECT
    test_date,
    state,
    GEOID,
    county_name,
    ip,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name, ip
),
# Final upload stats per day
ul_stats_per_day AS (
  SELECT 
    test_date,
    state,
    GEOID,
    county_name,
    MIN(MIN_upload_Mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS MAX_upload_Mbps
  FROM
    ul_stats_perip_perday
  GROUP BY test_date, state, GEOID, county_name
),
ul_total_samples_per_geo AS (
  SELECT
    test_date,
    COUNT(*) AS ul_total_samples,
    state,
    GEOID,
    county_name
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, GEOID, county_name
)
SELECT * FROM dl_histogram
JOIN ul_histogram USING (test_date, state, GEOID, county_name, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (test_date, state, GEOID, county_name)
JOIN dl_total_samples_per_geo USING (test_date, state, GEOID, county_name)
JOIN ul_stats_per_day USING (test_date, state, GEOID, county_name)
JOIN ul_total_samples_per_geo USING (test_date, state, GEOID, county_name)
ORDER BY test_date, state, GEOID, county_name, bucket_min, bucket_max