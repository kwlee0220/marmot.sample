{
  "name": "sample expand1",
  "operator": [
    <load("교통/지하철/서울역사")>,
    <expand1("area", "DOUBLE", "ST_Area(the_geom)")>,
    <expand("the_geom", "POINT")>,
    <expand("sig_cd", "INT")>,
    <project("the_geom,area,sig_cd")>
  ]
}