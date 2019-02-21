{
  "name": "인구밀도_2017_중심점추출_10000이상",
  "operators": [
    <load("주민/인구밀도_2000")>,
    <centroid()>,
    <filter("value >= 10000")>,
    <project("the_geom")>
  ]
}