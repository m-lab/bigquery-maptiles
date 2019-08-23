local rows = import 'results.json';
{
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      geometry: {
        type: "Point",
        // TODO: eliminate this by using a custom process to run the query and reformat results.
        coordinates: [std.parseInt(row.longitude) / 1000, std.parseInt(row.latitude) / 1000],
      },
      properties: {
        name: row.site,
        count: std.parseInt(row.count),
        download_Mbps: std.parseInt(row.download_Mbps) / 1000,
        min_rtt: std.parseInt(row.min_rtt),
      }
    }
    for row in rows
  ]
}
