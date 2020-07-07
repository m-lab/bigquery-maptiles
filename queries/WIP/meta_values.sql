WITH metavalues AS (
  SELECT 
      controlMeta.Value AS meta_val, controlMeta.Name AS meta_name, 
  FROM `measurement-lab.ndt.ndt5`as ndt5 
    CROSS JOIN UNNEST(ndt5.result.Control.ClientMetadata) as controlMeta
  WHERE ControlMeta.Name LIKE '%os%'
  # to see just the counts of one client app, uncomment below and change the value to the client app string
  #AND partition_date = '2020-03-19' #controlMeta.Value = 'NDTjs-PW-NDIA'
  GROUP BY meta_val, meta_name
)
SELECT * FROM metavalues
