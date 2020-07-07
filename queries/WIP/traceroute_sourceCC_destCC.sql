# Count the number of times requests made within (and to) a country leave their country of origin
WITH traces AS (
  SELECT
    DATE(TestTime) as date,
    uuid,
    traceroute.Source.geo.country_code AS source_country_code,
    traceroute.Destination.geo.country_code AS destination_country_code,
    hops.Source.IP AS hop_ip,
    hops.Source.CountryCode AS hop_country_code
  FROM `measurement-lab.ndt.traceroute` AS traceroute,
       UNNEST(traceroute.Hop) as hops
  WHERE hops.Source.CountryCode IS NOT NULL AND hops.Source.CountryCode != ""
)
SELECT
 COUNTIF(source_country_code != destination_country_code) AS count_sourceCC_not_equal_destCC,
 COUNTIF(source_country_code != hop_country_code) AS count_sourceCC_not_equal_hopCC,
 source_country_code,
 destination_country_code
FROM traces
WHERE date BETWEEN "2020-06-01" AND "2020-06-29"
AND source_country_code IS NOT NULL AND source_country_code != ""
AND destination_country_code IS NOT NULL AND destination_country_code != ""
GROUP BY source_country_code, destination_country_code
ORDER BY source_country_code, destination_country_code