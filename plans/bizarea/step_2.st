{
  "name": "대도시 상업지역 100mX100m 그리드 구역 생성",
  "operators": [
    <loadGrid("구역/연속지적도", "100", "100")>,
    <spatialSemiJoin("tmp/bizarea/area")>,
    <spatialJoin("tmp/bizarea/big_cities", "*-{cell_pos},param.sgg_cd")>,
    <spatialJoin("구역/지오비전_집계구_Point", "*-{cell_pos},param.block_cd")>
  ]
}