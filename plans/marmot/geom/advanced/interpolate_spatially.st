{
  "name": "InterpolateSpatially 테스트",
  "operators": [
    <load("tmp/points")>,
    <interpolate_spatially("tmp/points", "value", "3000", "value", "idw:power:1.000000")>
  ]
}