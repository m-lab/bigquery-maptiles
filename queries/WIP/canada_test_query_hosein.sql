WITH canada_2020 AS (
  SELECT
    tests.test_date,
    tests.client.geo.country_code AS country_code,
    tests.client.geo.country_name AS country_name,
    tests.client.geo.region AS province,
    tests.client.geo.city AS city,
    tcpinfo.server.IATA AS destination_iata,
    tcpinfo.FinalSnapshot.TCPInfo.BytesAcked AS bytesAcked,
    tcpinfo.FinalSnapshot.TCPInfo.BytesReceived AS bytesReceived
  FROM `measurement-lab.ndt.unified_downloads` tests, `measurement-lab.ndt.tcpinfo` tcpinfo
  WHERE
    test_date >= '2020-01-01'
    AND tests.client.geo.country_code = "CA"
    AND tests.client.geo.country_code IS NOT NULL AND tests.client.geo.country_code != ''
    AND tests.client.geo.country_name IS NOT NULL AND tests.client.geo.country_name != ''
    AND tests.client.geo.city IS NOT NULL AND tests.client.geo.city != ''
    AND tests.a.UUID = tcpinfo.UUID
)
SELECT
  CASE WHEN CHAR_LENGTH(CAST(EXTRACT(ISOWEEK FROM test_date) AS STRING)) < 2
    THEN CONCAT(EXTRACT(ISOYEAR FROM test_date), "0", EXTRACT(ISOWEEK FROM test_date))
  ELSE CONCAT(EXTRACT(ISOYEAR FROM test_date), EXTRACT(ISOWEEK FROM test_date))
  END AS time_period,
  COUNTIF(destination_iata NOT IN ("yvr", "ywg", "yyc", "yul", "yyz", "yqm"))/COUNT(*) AS pct_dest_not_canada,
  COUNT(*) AS total_tests,
  SUM(bytesAcked) AS total_bytes_acked,
  SUM(bytesReceived) AS total_bytes_received,
  country_name, province, city, destination_iata
FROM canada_2020
GROUP BY time_period, country_name, province, city, destination_iata
ORDER BY time_period ASC, pct_dest_not_canada DESC, province, city, destination_iata ASC
