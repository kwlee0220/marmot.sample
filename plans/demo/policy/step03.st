{
  "name": "인구밀도_10000이상_행정동추출",
  "operators": [
    <load("구역/행정동코드")>,
    <project("the_geom")>,
    <spatialSemiJoin("tmp/10min/high_density_center")>,
    <project("the_geom")>
  ]
}