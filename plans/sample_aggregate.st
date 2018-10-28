{
  "name": "sample aggregate",
  "operators": [
    <load("POI/주유소_가격")>,
    <filter("휘발유 > 0")>,
    <aggregate(["count::count","max:휘발유:max", "avg:휘발유:avg", "stddev:휘발유:stddev",
    			"envelope:the_geom:the_geom"])>
  ]
}