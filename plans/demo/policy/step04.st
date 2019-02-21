{
  "name": "경로당필요지역추출",
  "operators": [
    <load("구역/연속지적도_2017")>,
    <project("the_geom,pnu")>,
    <spatial_semijoin("tmp/10min/eldely_care_facilites_bufferred", "true")>,
    <clip_join("tmp/10min/high_density_hdong")>,
    <shard("1")>
  ]
}