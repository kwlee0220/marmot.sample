{
  "name": "서울지역 지하철역사 1KM 버퍼",
  "operators": [
    <load("교통/지하철/서울역사")>,
    <filter("sig_cd.substring(0,2) == '11'")>,
    <buffer("1000")>
  ]
}