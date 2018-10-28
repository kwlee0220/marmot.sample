{
  "name": "sample spatial_seimijoin",
  "operators": [
    <load("POI/주유소_가격")>,
    <spatial_semijoin_dist("교통/지하철/역사", "30")>
  ]
}