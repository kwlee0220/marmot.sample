{
  "name": "연속지적도 추출",
  "operators": [
    <load("토지/개별공시지가_2017")>,
    <project("the_geom, pnu")>,
    <shard("1")>
  ]
}