{
  "name": "격자별_팀별_민원수",
  "operators": [
    <load("기타/안양대/도봉구/민원")>,
    <spatial_join("기타/안양대/도봉구/GRID_100", "team_name,param.{the_geom,spo_no_cd}")>,
    <aggregate_by_group("team_name,spo_no_cd", "count::count", "the_geom")>
  ]
}