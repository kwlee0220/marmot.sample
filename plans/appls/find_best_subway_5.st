@code{
updateExpr = "if ( normalized == null ) {"
			+ "  the_geom = param_geom;"
			+ "  cell_id = param_cell_id;"
			+ "  normalized = 0;"
			+ "} else if ( param_normalized == null ) {"
			+ "	 param_normalized = 0;"
			+ "}"
			+ "normalized = normalized + param_normalized;";
}	
	
{
  "name": "그리드 셀단위 유동인구 비율과 택시 승하차 로그 비율 합계 계산",
  "operators": [
    <load("분석결과/지하철역사_추천/유동인구")>,
    <fullOuterHashJoin("cell_id", "분석결과/지하철역사_추천/택시로그", "cell_id",
    		"the_geom,cell_id,normalized,param.{the_geom as param_geom,cell_id as param_cell_id,normalized as param_normalized}")>,
    <update("@{updateExpr}")>,
    <project("the_geom,cell_id,normalized as value")>
  ]
}