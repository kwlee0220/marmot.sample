{
  "name": "대도시 상업지역 구역별 건축물 수와 면적 집계",
  "operators": [
    <load("건물/통합정보")>,
    <spatialJoin("tmp/bizarea/grid100", "건축물용도코드,대지면적,param.*")>,
    <aggregateByGroup("cell_id,block_cd,건축물용도코드", ["sum:대지면적:대지면적","count::bld_cnt"],
    					"the_geom,sgg_cd", "3")>,
    <project("the_geom,*-{the_geom}")>
  ]
}