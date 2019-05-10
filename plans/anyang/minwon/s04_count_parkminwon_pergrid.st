{
  "name": "격자별 공원관련 민원수 합계",
  "operators": [
    <load("기타/안양대/도봉구/GRID_100")>,
    <spatialOuterJoin("기타/안양대/도봉구/공원_민원", "the_geom,spo_no_cd")>,
    <aggregateByGroup("spo_no_cd", "count::count", "the_geom")>
  ]
}