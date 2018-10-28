{
  "name": "서울특별시 영역 추출",
  "operators": [
    <load("구역/시도")>,
    <filter("ctprvn_cd == '11'")>,
    <project("the_geom")>
  ]
}