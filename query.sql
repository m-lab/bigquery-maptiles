#standardSQL

-- This query calculates the median download rate and MinRTT for every unique
-- combination of latitude and longitude for the previous day. Results are also
-- grouped by site to allow identifying geographic cohorts.
--
-- NOTE: Query depends on the web100 tables.

SELECT
  COUNT(*) AS count,
  REGEXP_EXTRACT(
    connection_spec.server_hostname, "mlab[1-4].([a-z]{3}[0-9]{2}).*") AS site,
  -- TODO: replace bq.
  -- NOTE: `bq` converts everything to strings, so we encode fixed-point values
  -- to 3 decimal places.
  CAST(APPROX_QUANTILES(
      8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsAcked,
      (web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd)), 100)[SAFE_ORDINAL(50)] * 1000 AS INT64) AS download_Mbps,
  APPROX_QUANTILES(web100_log_entry.snap.MinRTT, 100)[SAFE_ORDINAL(50)] AS min_rtt,
  CAST(connection_spec.client_geolocation.longitude * 1000 AS INT64) as longitude,
  CAST(connection_spec.client_geolocation.latitude * 1000 AS INT64) as latitude

FROM
  `measurement-lab.ndt.downloads`

WHERE
      partition_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND connection_spec.client_geolocation.longitude is not NULL
  AND connection_spec.client_geolocation.latitude is not NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  AND (web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin +
       web100_log_entry.snap.SndLimTimeCwnd +
       web100_log_entry.snap.SndLimTimeSnd) < 600000000

GROUP BY
  site,
  connection_spec.client_geolocation.longitude,
  connection_spec.client_geolocation.latitude
