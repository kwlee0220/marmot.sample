@code{
	sido_expr = "['11', '26', '27', '28', '29', '30', '31']";
	sgg_expr = "['41115','41111','41117', '41113', '48125', '48123', '48127', '48121', '48129', '41281', '41285', '41287']";
	init_expr = String.format("$sid_cd=%s; $sgg_cd=%s", sido_expr, sgg_expr);
}
{
  "name": "대도시지역 추출",
  "operators": [
    <load("구역/통합법정동")>,
    <define_column("sid_cd:string", "bjd_cd.substring(0,2)")>,
    <define_column("sgg_cd:string", "bjd_cd.substring(0,5)")>,
    <filter("$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)", "@{init_expr}")>
  ]
}