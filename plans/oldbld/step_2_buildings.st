@code{
	trans = "$date = null;"
			+ "if ( 사용승인일자 != null && 사용승인일자.length() == 8 ) {"
				+ "if ( 사용승인일자.endsWith('00') ) { 사용승인일자 = 사용승인일자.substring(0,6) + '01' }"
				+ "$date = DateParseLE(사용승인일자, $pat);"
			+ "}"
			+ "else { $date = null; }"
			+ "if ( $date != null ) {"
				+ "$age = (DateDaysBetween($date,$now)) / 365L;"
				+ "old = $age >= 20 ? 1 : 0;"
				+ "be5 = $age >= 5 ? 1 : 0;"
			+ "}"
			+ "else { old = 0; be5 = 0; }";
}
{
  "name": "행정구역당 20년 이상된 건물 집계",
  "operators": [
    <load("건물/통합정보")>,
    <defineColumn("emd_cd:string", "pnu.substring(0,8)")>,
    <expand("old:byte,be5:byte", "@{trans}", "$now = DateNow(); $pat = DatePattern('yyyyMMdd');")>,
    <project("emd_cd,old,be5")>,
    <aggregateByGroup("emd_cd",["sum:old:old_cnt", "sum:be5:be5_cnt", "count::bld_cnt"], [], "1")>,
    <defineColumn("old_ratio:double", "(double)old_cnt/bld_cnt")>
  ]
}