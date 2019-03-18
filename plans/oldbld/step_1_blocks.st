{
  "name": "읍면동별 집계구 수집",
  "operators": [
    <load("구역/지오비전_집계구_Point")>,
    <spatial_join("구역/읍면동", "param.emd_cd,block_cd")>
  ]
}