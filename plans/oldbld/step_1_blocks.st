{
  "name": "읍면동별 집계구 수집",
  "operators": [
    <load("구역/지오비전_집계구_Point")>,
    <spatialJoin("구역/읍면동", "param.emd_cd,block_cd")>
  ]
}