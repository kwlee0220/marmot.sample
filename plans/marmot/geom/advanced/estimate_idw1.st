{
  "name": "EstimateIDW 테스트",
  "operators": [
    <load("주민/인구밀도_2000")>,
    <centroid()>,
    <project("the_geom, big_sq, value")>
  ]
}