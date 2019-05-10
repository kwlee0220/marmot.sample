{
  "name": "EstimateIDW 테스트2",
  "operators": [
    <load("tmp/points")>,
    <estimate_idw("tmp/points", "value", "10", "5000", "value")>
  ]
}