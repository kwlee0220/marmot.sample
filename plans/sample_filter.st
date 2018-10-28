{
  "name": "sample filter",
  "operators": [
    <load("POI/주유소_가격")>,
    <filter("휘발유 > 2000")>,
    <project("the_geom,상호,휘발유")>
  ]
}