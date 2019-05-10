{
  "name": "sample spatialSemiJoinDist",
  "operators": [
    <load("POI/주유소_가격")>,
    <spatialSemiJoinDist("교통/지하철/역사", "30")>
  ]
}
