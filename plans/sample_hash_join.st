{
  "name": "sample hash_join",
  "operators": [
    <load("구역/시군구")>,
    <update("sig_cd = sig_cd.substring(0,2)")>,
    <hashJoin("sig_cd", "구역/시도", "ctprvn_cd",
    		"the_geom,param.ctp_kor_nm,sig_kor_nm", "5")>
  ]
}
