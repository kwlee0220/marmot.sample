{
  "name": "대도시 상업지역 100mX100m 그리드 구역 생성",
  "operators": [
    <load_grid("구역/연속지적도", "100", "100")>,
    <spatial_semijoin("tmp/bizarea/area")>,
    <spatial_join("tmp/bizarea/big_cities", "*-{cell_pos},param.sgg_cd")>,
    <spatial_join("구역/지오비전_집계구_Point", "*-{cell_pos},param.block_cd")>
  ]
}