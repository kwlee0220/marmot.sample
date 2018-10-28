{
  "name": "격자별_팀별_민원수",
  "operators": [
    <load("기타/안양대/도봉구/민원")>,
    <spatial_join("기타/안양대/도봉구/GRID_100", "team_name,param.{the_geom,spo_no_cd}")>,
    <aggregate_by_group("team_name,spo_no_cd", "count::count", "the_geom")>
  ]
}

	private static final String MINWON = "기타/안양대/도봉구/민원";
	private static final String GRID = "기타/안양대/도봉구/GRID_100";
	private static final String OUTPUT = "분석결과/안양대/도봉구/격자별_팀별_민원수";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		Plan plan;
		plan = marmot.planBuilder("격자별_팀별_민원수")
					.load(MINWON)
					.filter("team_name != null")
					.spatialJoin("the_geom", GRID,
									"team_name,param.{the_geom,spo_no_cd}")
					.groupBy("team_name,spo_no_cd")
						.tagWith("the_geom")
						.count()
					.build();
		GeometryColumnInfo gcInfo = marmot.getDataSet(MINWON).