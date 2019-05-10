@code{
	String avg;
	StringBuilder builder = new StringBuilder();
	for ( int i =0; i < 24; ++i ) {
	    avg = String.format("avg_%02dtmst", i);
		builder.append(avg);
		if ( i < 23 ) {
			builder.append('+');
		}
	}
	avgExpr = String.format("(%s)/24", builder);
}
{
  "name": "대도시 상업지역 구역별 유동인구수 집계",
  "operators": [
    <load("주민/유동인구/월별_시간대/2015")>,
    <defineColumn("flow_pop:double", "@{avgExpr}")>,
    <project("std_ym,block_cd,flow_pop")>,
    <hashJoin("block_cd", "tmp/bizarea/grid100", "block_cd", "param.*,std_ym,flow_pop", "32")>,
    <aggregateByGroup("std_ym,cell_id", "avg:flow_pop:flow_pop", "the_geom,sgg_cd", "3")>,
    <project("the_geom,*-{the_geom}")>
  ]
}