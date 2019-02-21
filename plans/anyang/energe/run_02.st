{
  "name": "2017년 월별 전기 사용량 합계",
  "operators": [
    <load("건물/건물에너지/전기사용량")>,
    <define_column("year:short", "사용년월.substring(0, 4)")>,
    <filter("year == 2017")>,
    <define_column("month:short", "사용년월.substring(4, 6)")>,
    <update("사용량 = Math.max(사용량, 0)")>,
    <aggregate_by_key("pnu,month","sum:사용량:usage", [], "1")>,
    <project("pnu, month,  usage")>
  ]
}