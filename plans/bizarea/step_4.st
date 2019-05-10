@code{
	String amt;
	StringBuilder builder = new StringBuilder();
	for ( int i =0; i < 24; ++i ) {
	    amt = String.format("sale_amt_%02dtmst", i);
		builder.append(amt);
		if ( i < 23 ) {
			builder.append('+');
		}
	}
	sumExpr = builder.toString();
}
{
  "name": "대도시 상업지역 구역별 카드 일매출 집계",
  "operators": [
    <load("주민/카드매출/월별_시간대/2015")>,
    <defineColumn("daily_sales:double", "@{sumExpr}")>,
    <project("std_ym,block_cd,daily_sales")>,
    <hashJoin("block_cd", "tmp/bizarea/grid100", "block_cd", "param.*,std_ym,daily_sales", "64")>,
    <aggregateByGroup("std_ym,cell_id", "sum:daily_sales:daily_sales", "the_geom,sgg_cd", "3")>,
    <project("the_geom,*-{the_geom}")>
  ]
}