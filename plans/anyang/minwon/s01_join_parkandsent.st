{
  "name": "이름기반 공원 감성분석 맵매칭",
  "operators": [
    <load("기타/안양대/도봉구/공원")>,
    <filter("!kor_par_nm.equals('#N/A')")>,
    <hashJoin("kor_par_nm", "기타/안양대/도봉구/공원_감성분석", "poi", "the_geom,param.*-{the_geom}")>
  ]
}
