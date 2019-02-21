{
  "name": "노인복지시설_경로당_추출_버퍼",
  "operators": [
    <load("POI/노인복지시설")>,
    <filter("induty_nm == '경로당'")>,
    <buffer("400")>,
    <project("the_geom")>
  ]
}