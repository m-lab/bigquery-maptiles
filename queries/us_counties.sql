#standardSQL
WITH counties AS (
  SELECT
    county_name AS name,
    county_geom AS WKT,
    geo_id as geo_id
  FROM
    `bigquery-public-data.geo_us_boundaries.counties`
),
mlab_dl AS (
  SELECT
    tests.*,
    EXTRACT(YEAR FROM test_date) AS year,
    EXTRACT(MONTH FROM test_date) AS month,
    EXTRACT(WEEK FROM test_date) AS week,
    counties.geo_id AS geo_id,
    counties.name AS county_name,
    counties.WKT AS WKT
  FROM
    `measurement-lab.library.ndt_unified_downloads` tests
    JOIN counties ON ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), WKT
    )
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
),
mlab_ul AS (
  SELECT
    tests.*,
    counties.geo_id AS geo_id,
    counties.name AS county_name,
    counties.WKT AS WKT
  FROM
    `mlab-sandbox.library.ndt_unified_uploads` tests
    JOIN counties ON ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ),
      WKT
    )
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'

),
dl_agg AS (
  SELECT
  mlab_dl.geo_id,
  county_name,
  EXTRACT(YEAR FROM test_date) AS year,
  EXTRACT(MONTH FROM test_date) AS month,
  EXTRACT(WEEK FROM test_date) AS week,
  MIN(a.MeanThroughputMbps) AS MIN_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(26)] AS LOWER_QUARTILE_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(51)] AS MED_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(76)] AS UPPER_QUARTILE_download_Mbps,
  MAX(a.MeanThroughputMbps) AS MAX_download_Mbps,  
  AVG(a.MeanThroughputMbps) AS AVG_download_Mbps,
  APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 101) [ORDINAL(51)] as MED_min_rtt
  FROM mlab_dl JOIN counties ON mlab_dl.county_name = counties.name
  GROUP BY geo_id, county_name, year, month, week
),
ul_agg AS (
  SELECT
  mlab_ul.geo_id,
  county_name,
  EXTRACT(YEAR FROM test_date) AS year,
  EXTRACT(MONTH FROM test_date) AS month,
  EXTRACT(WEEK FROM test_date) AS week,
  MIN(a.MeanThroughputMbps) AS MIN_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(26)] AS LOWER_QUARTILE_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(51)] AS MED_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(76)] AS UPPER_QUARTILE_upload_Mbps,     
  MAX(a.MeanThroughputMbps) AS MAX_upload_Mbps,   
  AVG(a.MeanThroughputMbps) 
    AS AVG_upload_Mbps
  FROM mlab_ul JOIN counties ON mlab_ul.county_name = counties.name
  GROUP BY geo_id, county_name, year, month, week
),
summary AS (
  SELECT
    geo_id AS county_geoid, county_name, year, month, week, MIN_download_Mbps, LOWER_QUARTILE_download_Mbps, MED_download_Mbps,
    UPPER_QUARTILE_download_Mbps, MAX_download_Mbps, AVG_download_Mbps, MED_min_rtt, MIN_upload_Mbps, LOWER_QUARTILE_upload_Mbps, 
    MED_upload_Mbps, UPPER_QUARTILE_upload_Mbps, MAX_upload_Mbps, AVG_upload_Mbps
  FROM
  dl_agg JOIN ul_agg USING (geo_id, county_name, year, month, week)
)
SELECT * FROM summary JOIN counties ON summary.county_geoid = counties.geo_id