{
  "name": "sample spatialJoin",
  "operators": [
    <load("POI/주유소_가격")>,
    <spatialJoin("구역/읍면동", "*,param.emd_kor_nm as emd_name")>
  ]
}
