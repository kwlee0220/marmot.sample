{
  "name": "이름기반 공원 감성분석 맵매칭_ID",
  "operators": [
    <load("기타/안양대/도봉구/공원")>,
    <filter("!kor_par_nm.equals('#N/A')")>,
    <hashJoin("id", "기타/안양대/도봉구/공원_감성분석", "id", "the_geom,param.*-{the_geom}")>,
    <project("the_geom,row_id,poi,id,sp,sn,언급빈도수 as mention,선호도 as preference")>
  ]
}