{
  "name": "'택시 운행 로그'에서 500mX500m 격자단위 승하차 집계",
  "operators": [
    <load("로그/나비콜/택시로그")>,
    <filter("status == 1 || status == 2")>,
    <intersects("tmp/seoul")>,
    <project("the_geom")>,
    <spatialSemiJoin("분석결과/지하철역사_추천/지하철역사_버퍼_그리드", "true")>,
    <assignGridCell("tmp/seoul", "500", "500")>,
    <project("cell_geom as the_geom, cell_id, cell_pos")>,
    <aggregateByGroup("cell_id", "count::count", "the_geom")>
  ]
}