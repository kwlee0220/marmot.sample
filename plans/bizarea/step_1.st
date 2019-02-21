@code{
	init_expr = "$types = ['일반상업지역', '유통상업지역', '근린상업지역', '중심상업지역']";
}
{
  "name": "상업지역 추출",
  "operators": [
    <load("토지/용도지역지구")>,
    <filter("$types.contains(dgm_nm)", "@{init_expr}")>,
    <project("the_geom")>
  ]
}