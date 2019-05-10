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
  "name": "결과 통합",
  "operators": [
    <loadFullOuterHashJoin("tmp/oldbld/buildings_emd", "emd_cd",
    							"tmp/oldbld/card_sale_emd", "emd_cd",
    							"left.*,right.*-{emd_cd}", "1")>,
    <fullOuterHashJoin("emd_cd", "tmp/oldbld/pop_emd", "emd_cd", "*,param.*-{emd_cd}", "1")>,
    <hashJoin("emd_cd", "구역/읍면동", "emd_cd", "param.the_geom,*", "1")>
  ]
}