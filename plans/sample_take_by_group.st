{
  "name": "sample listByGroup",
  "operators": [
    <load("교통/지하철/서울역사")>,
    <takeByGroup("sig_cd", "sub_sta_sn:A", "3")>,
    <project("sig_cd, sub_sta_sn")>
  ]
}
