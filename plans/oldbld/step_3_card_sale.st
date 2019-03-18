@code{
	StringBuilder builder = new StringBuilder();
	for ( int i =0; i < 24; ++i ) {
		builder.append(String.format("sale_amt_%02dtmst", i));
		if ( i < 23 ) {
			builder.append('+');
		}
	}
	sumExpr = builder.toString();
}
{
  "name": "읍면동별 2015년도 카드매출 집계",
  "operators": [
    <load("주민/카드매출/월별_시간대/2015")>,
    <define_column("sale_amt:double", "@{sumExpr}")>,
    <project("block_cd,sale_amt")>,
    <aggregate_by_key("block_cd",["sum:sale_amt:sale_amt"])>,
    <hash_join("block_cd", "tmp/oldbld/blocks_emd", "block_cd", "*,param.{emd_cd}")>,
    <aggregate_by_key("emd_cd",["sum:sale_amt:sale_amt"], [], "1")>
  ]
}