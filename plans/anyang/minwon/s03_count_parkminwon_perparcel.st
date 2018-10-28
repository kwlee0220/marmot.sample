{
  "name": "이름기반 공원 감성분석 맵매칭",
  "operators": [
    <load("기타/안양대/도봉구/공원_민원")>,
    <right_outerjoin("all_parcel_layer_id", "기타/안양대/도봉구/필지", "id", "param.{the_geom,id},team_name")>,
    <aggregate_by_group("id", "count::count", "the_geom")>
  ]
}
	
