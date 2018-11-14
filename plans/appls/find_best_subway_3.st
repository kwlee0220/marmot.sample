@code{
	String avg;
	StringBuilder builder = new StringBuilder();
	StringBuilder builder2 = new StringBuilder();
	for ( int i =0; i < 24; ++i ) {
	    avg = String.format("avg_%02dtmst", i);
	    
		builder.append(avg);
		if ( i < 23 ) {
			builder.append('+');
		}
		
		builder2.append(String.format("if ( %s == null ) { %s = 0 } ", avg, avg));
	}
	sumExpr = builder.toString();
	updateExpr = builder2.toString();
}
{
  "name": "'500mX500m 격자단위 유동인구' 집계",
  "operators": [
    <query("주민/유동인구/월별_시간대/2015", "tmp/seoul")>,
    <spatial_semijoin("분석결과/지하철역사_추천/지하철역사_버퍼_그리드", "true")>,
    <update("@{updateExpr}")>,
    <expand("day_total", "DOUBLE", "@{sumExpr}")>,
    <aggregate_by_key("block_cd", "avg:day_total:avg", "the_geom")>,
    <assign_gridcell("tmp/seoul", "500", "500")>,
    <project("cell_geom as the_geom, cell_id, cell_pos, avg")>,
    <aggregate_by_key("cell_id", "sum:avg:avg", "the_geom")>
  ]
}