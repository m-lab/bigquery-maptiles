SELECT * FROM `measurement-lab.mlab_statistics.continent_country_region_maxDL_histogram`
WHERE continent_code = '@continent_code'
AND country_code = '@country_code'
AND ISO3166_2region1 = '@country_code-@region_code'