{
  "name": "sample aggregate_by_group",
  "operators": [
    <load("교통/지하철/서울역사")>,
    <filter("sig_cd.startsWith('11')")>,
    <aggregateByGroup("sig_cd",
					["count::count", "max:sub_sta_sn:max_sn", "min:sub_sta_sn:min",
					"sum:sub_sta_sn:sum", "avg:sub_sta_sn:avg", "stddev:sub_sta_sn:stddev"])>
  ]
}
