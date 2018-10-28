{
  "name": "sample spatial_join",
  "operators": [
    <load("POI/주유소_가격")>,
    <spatial_join("구역/읍면동", "*,param.emd_kor_nm as emd_name")>
  ]
}