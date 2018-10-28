{
  "name": "sample spatial_join",
  "operators": [
    <load_spatial_indexjoin("교통/지하철/서울역사", "구역/시군구", "left.*,right.{the_geom as the_geom2}")>,
    <intersection("the_geom", "the_geom2", "the_geom")>,
    <project("*-{the_geom2}")>
  ]
}