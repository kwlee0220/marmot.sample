{
  "name": "2017년 월별 전기 사용량 합계",
  "operators": [
    <load("건물/건물에너지/전기사용량")>,
    <defineColumn("year:short", "사용년월.substring(0, 4)")>,
    <filter("year == 2017")>,
    <defineColumn("month:short", "사용년월.substring(4, 6)")>,
    <update("사용량 = Math.max(사용량, 0)")>,
    <aggregateByGroup("pnu,month","sum:사용량:usage", [], "1")>,
    <project("pnu, month,  usage")>
  ]
}