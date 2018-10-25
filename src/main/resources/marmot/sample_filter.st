{
  "name": "sample filter",
  "operator": [
    <load("POI/주유소_가격")>,
    <filter0("휘발유 > 2000")>,
    <project("the_geom,상호,휘발유")>
  ]
}