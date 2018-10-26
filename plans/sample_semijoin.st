{
  "name": "sample semijoin",
  "operator": [
    <load("구역/시군구")>,
    <semi_join("sig_cd", "교통/지하철/서울역사", "sig_cd")>
  ]
}