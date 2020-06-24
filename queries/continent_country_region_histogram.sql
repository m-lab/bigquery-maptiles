WITH
# Select the initial set of results
per_location AS (
  SELECT
    test_date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    a.MeanThroughputMbps AS mbps,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE test_date = @startday
),
# With good locations and valid IPs
per_location_cleaned AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    mbps,
    ip          
  FROM per_location
  WHERE 
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND ip IS NOT NULL
),
# Descriptive statistics per IP, per day 
desc_stats_per_day_ip AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ip,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps  FROM per_location_cleaned
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ip
),
# Count the samples
sample_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    COUNT(*) AS samples
  FROM desc_stats_per_day_ip
  GROUP BY
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1  
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10,x) AS bucket_right, POW(10, x-.2) AS bucket_left
  FROM UNNEST(GENERATE_ARRAY(0, 3, .2)) AS x
),
# Count the samples that fall into each bucket
histogram_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    bucket_right AS bucket,
    COUNTIF(MIN_download_Mbps < bucket_right 
            AND MIN_download_Mbps >= bucket_left) AS MIN_DL_bucket_count,
    COUNTIF(LOWER_QUART_download_Mbps < bucket_right 
            AND LOWER_QUART_download_Mbps >= bucket_left) AS LOWER_QUART_DL_bucket_count,
    COUNTIF(MED_download_Mbps < bucket_right 
            AND MED_download_Mbps >= bucket_left) AS MED_DL_bucket_count,
    COUNTIF(MEAN_download_Mbps < bucket_right 
            AND MEAN_download_Mbps >= bucket_left) AS MEAN_DL_bucket_count,
    COUNTIF(UPPER_QUART_download_Mbps < bucket_right 
            AND UPPER_QUART_download_Mbps >= bucket_left) AS UPPER_QUART_DL_bucket_count,
    COUNTIF(MAX_download_Mbps < bucket_right 
            AND MAX_download_Mbps >= bucket_left) AS MAX_DL_bucket_count
  FROM desc_stats_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1, 
    bucket
),
# Turn the counts into frequencies
histogram AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1, 
    bucket AS mbps, 
    MIN_DL_bucket_count / samples AS MIN_DL_frac, 
    LOWER_QUART_DL_bucket_count / samples AS LOWER_QUART_DL_frac,
    MED_DL_bucket_count / samples AS MED_DL_frac,
    MEAN_DL_bucket_count / samples AS MEAN_DL_frac, 
    UPPER_QUART_DL_bucket_count / samples AS UPPER_QUART_DL_frac,
    MAX_DL_bucket_count / samples AS MAX_DL_frac,
    samples
  FROM histogram_counts 
  JOIN sample_counts USING (test_date, continent_code, country_code,
                            country_name, ISO3166_2region1)
)
# Show the results
SELECT * FROM histogram
ORDER BY test_date, continent_code, country_code,
         country_name, ISO3166_2region1, mbps
