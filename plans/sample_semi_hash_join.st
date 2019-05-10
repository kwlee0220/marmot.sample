{
  "name": "sample semiHashJoin",
  "operators": [
    <load("구역/시군구")>,
    <semiHashJoin("sig_cd", "교통/지하철/서울역사", "sig_cd")>
  ]
}
