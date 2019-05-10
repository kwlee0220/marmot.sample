{
  "name": "담당팀_필지별_민원수 합계",
  "operators": [
    <load("기타/안양대/도봉구/민원")>,
    <rightOuterHashJoin("all_parcel_layer_id", "기타/안양대/도봉구/필지", "id",
    						"param.{the_geom,id},team_name")>,
    <aggregateByGroup("team_name,id", "count::count", "the_geom")>
  ]
}