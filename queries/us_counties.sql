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
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS ymw,
    counties.geo_id AS dl_geo_id
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
    AND test_date >= '2019-12-29'
),
mlab_ul AS (
  SELECT
    tests.*,
    CONCAT(EXTRACT(YEAR FROM test_date),"-", EXTRACT(MONTH FROM test_date), "-",
      EXTRACT(WEEK FROM test_date)) AS ymw,
    counties.geo_id AS ul_geo_id
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
    AND test_date >= '2019-12-29'

),
dl_agg AS (
  SELECT
  mlab_dl.dl_geo_id AS dl_geo_id,
  ymw AS dl_ymw,
  MIN(a.MeanThroughputMbps) AS MIN_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(26)] AS LOWER_QUARTILE_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(51)] AS MED_download_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps, 101) [SAFE_ORDINAL(76)] AS UPPER_QUARTILE_download_Mbps,
  MAX(a.MeanThroughputMbps) AS MAX_download_Mbps,  
  AVG(a.MeanThroughputMbps) AS AVG_download_Mbps,
  APPROX_QUANTILES(CAST(a.MinRTT AS FLOAT64), 101) [ORDINAL(51)] as MED_min_rtt
  FROM mlab_dl
  GROUP BY dl_geo_id, dl_ymw
),
ul_agg AS (
  SELECT
  mlab_ul.ul_geo_id AS ul_geo_id,
  ymw,
  MIN(a.MeanThroughputMbps) AS MIN_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(26)] AS LOWER_QUARTILE_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(51)] AS MED_upload_Mbps,
  APPROX_QUANTILES(a.MeanThroughputMbps,101) [SAFE_ORDINAL(76)] AS UPPER_QUARTILE_upload_Mbps,     
  MAX(a.MeanThroughputMbps) AS MAX_upload_Mbps,   
  AVG(a.MeanThroughputMbps) 
    AS AVG_upload_Mbps
  FROM mlab_ul
  GROUP BY ul_geo_id, ymw
),
summary AS (
  SELECT
    dl_geo_id, ymw, MIN_download_Mbps, LOWER_QUARTILE_download_Mbps, MED_download_Mbps,
    UPPER_QUARTILE_download_Mbps, MAX_download_Mbps, AVG_download_Mbps, MED_min_rtt, MIN_upload_Mbps, LOWER_QUARTILE_upload_Mbps, 
    MED_upload_Mbps, UPPER_QUARTILE_upload_Mbps, MAX_upload_Mbps, AVG_upload_Mbps
  FROM
  dl_agg JOIN ul_agg ON dl_geo_id = ul_agg.ul_geo_id
  JOIN counties ON dl_geo_id = counties.geo_id
)
SELECT * FROM summary
