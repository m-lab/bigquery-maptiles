WITH
# The last 5 weeks of data
per_location AS (
  SELECT
    test_date,
    client.Geo.country_name as location,
    a.MeanThroughputMbps as mbps,
    NET.SAFE_IP_FROM_STRING(Client.IP) as ip
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE test_date > DATE_SUB(DATE_TRUNC(CURRENT_DATE(), DAY), INTERVAL 5 WEEK)
),
# With good locations and valid IPs
per_ip AS (
        SELECT
           test_date,
           location,
           mbps,
           ip          
        FROM per_location
        WHERE location != "Unknown" AND location != "" AND location IS NOT NULL AND ip IS NOT NULL
),
# Normalized to one result per IP per day - we take the average here, but the median or even the max are also all defensible choices.
avg_per_ip as (
        SELECT test_date, location, AVG(mbps) as mbps
        FROM per_ip
        WHERE ip IS NOT NULL
        GROUP BY ip, test_date, location
),
# Count the samples
sample_counts AS (
    SELECT test_date, location, COUNT(*) as samples,
    FROM avg_per_ip
    GROUP BY test_date, location
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10,x) AS bucket_right, POW(10, x-.2) AS bucket_left
  FROM UNNEST(GENERATE_ARRAY(0, 3, .2)) AS x
),
# Count the samples that fall into each bucket
histogram_counts AS (
  SELECT test_date, location, bucket_right as bucket, COUNTIF(mbps < bucket_right AND mbps >= bucket_left) as bucket_count,
  FROM avg_per_ip CROSS JOIN buckets
  GROUP BY test_date, location, bucket
  ORDER BY test_date, location, bucket
),
# Turn the counts into frequencies
histogram AS (
  SELECT test_date, location, bucket as mbps, bucket_count / samples as frac, samples
  FROM histogram_counts JOIN sample_counts USING (test_date, location)
)
# Show the results
SELECT * FROM histogram
ORDER BY location, test_date, mbps