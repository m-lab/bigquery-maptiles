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
  WHERE test_date = "2020-06-01"
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
    MAX(mbps) AS MAX_download_Mbps
  FROM per_location_cleaned
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
    COUNTIF(MAX_download_Mbps < bucket_right 
            AND MAX_download_Mbps >= bucket_left) AS bucket_count
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
    bucket AS max_mbps, 
    bucket_count / samples AS max_frac, samples AS max_samples
  FROM histogram_counts 
  JOIN sample_counts USING (test_date, continent_code, country_code,
                            country_name, ISO3166_2region1)
)
# Show the results
SELECT * FROM histogram
ORDER BY test_date, continent_code, country_code,
         country_name, ISO3166_2region1
