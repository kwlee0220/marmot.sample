@code{
	StringBuilder builder = new StringBuilder();
	for ( int i =0; i < 24; ++i ) {
		builder.append(String.format("avg_%02dtmst", i));
		if ( i < 23 ) {
			builder.append('+');
		}
	}
	avgExpr = String.format("(%s)/24", builder);
}
{
  "name": "읍면동별 2015년도 평균 유동인구 집계",
  "operators": [
    <load("주민/유동인구/월별_시간대/2015")>,
    <define_column("avg:double", "@{avgExpr}")>,
    <project("block_cd,avg")>,
    <aggregate_by_key("block_cd",["avg:avg:avg"])>,
    <hash_join("block_cd", "tmp/oldbld/blocks_emd", "block_cd", "*,param.{emd_cd}")>,
    <aggregate_by_key("emd_cd",["avg:avg:pop_avg"], [], "1")>
  ]
}