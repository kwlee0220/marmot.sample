package apttrx;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.plan.GeomOpOption;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SummarizeByHighSchoolShort {
	private static final String APT_TRADE_TRX = "주택/실거래/아파트매매";
	private static final String APT_LEASE_TRX = "주택/실거래/아파트전월세";
	private static final String APT_LOC = "주택/실거래/아파트위치";
	private static final String SCHOOLS = "POI/전국초중등학교";
	private static final String HIGH_SCHOOLS = "tmp/아파트실매매/고등학교";
	private static final String TEMP = "tmp/tmp";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		Plan plan;
		
		//전국초중등학교 정보에서 고등학교만 뽑는다.
		DataSet school = marmot.getDataSetOrNull(HIGH_SCHOOLS);
		if ( school == null ) {
			school = findHighSchool(marmot);
		}
		System.out.println("done: 고등학교 위치 추출, elapsed=" + watch.getElapsedMillisString());
		
		RecordSchema schema;
		String geomCol = school.getGeometryColumn();

		Plan plan1 = countTradeTransaction(marmot);
		Plan plan2 = countLeaseTransaction(marmot);

		marmot.createDataSet(TEMP, school.getGeometryColumnInfo(), plan1, DataSetOption.FORCE);
		marmot.execute(plan2);
		System.out.println("done: 아파트 거래 정보 지오코딩, elapsed=" + watch.getElapsedMillisString());
		
		plan = marmot.planBuilder("고등학교_주변_거래_집계")
						.load(TEMP)
						// 고등학교를 기준으로 그룹핑하여 집계한다.
						.groupBy("id")
							.tagWith(geomCol + ",name")
							.aggregate(SUM("trade_count").as("trade_count"),
										SUM("lease_count").as("lease_count"))
						.expand1("count:long", "trade_count+lease_count")
						.sort("count:D")
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, school.getGeometryColumnInfo(), plan1, DataSetOption.FORCE);
		watch.stop();
		
		marmot.deleteDataSet(TEMP);
		
		SampleUtils.printPrefix(result, 3);
		System.out.printf("elapsed: %s%n", watch.getElapsedMillisString());
	}
	
	private static final Plan countTradeTransaction(MarmotRuntime marmot) {
		DataSet aptLoc = marmot.getDataSet(APT_LOC);
		String locGeomCol = aptLoc.getGeometryColumn();
		
		DataSet school = marmot.getDataSet(HIGH_SCHOOLS);
		String schoolGeomCol = school.getGeometryColumn();
		
		return marmot.planBuilder("고등학교_주변_아파트_매매_추출")
					.load(APT_LOC)
					
					// 고등학교 주변 1km 내의 아파트 검색.
					.centroid(locGeomCol)
					.buffer(locGeomCol, 1000, GeomOpOption.OUTPUT("circle"))
					.spatialJoin("circle", HIGH_SCHOOLS,
								String.format("*-{%s},param.{%s,id,name}",
											locGeomCol, schoolGeomCol))
					
					// 고등학교 1km내 위치에 해당하는 아파트 거래 정보를 검색.
					.join("시군구,번지,단지명", APT_TRADE_TRX, "시군구,번지,단지명",
							"the_geom,id,name,param.*", null)
					
					// 고등학교를 기준으로 그룹핑하여 집계한다.
					.groupBy("id")
						.tagWith(schoolGeomCol + ",name")
						.aggregate(COUNT().as("trade_count"))
					.expand1("lease_count:long", "0")
					.project("the_geom,id,name,trade_count,lease_count")
					
					.store(TEMP)
					.build();		
	}
	
	private static final Plan countLeaseTransaction(MarmotRuntime marmot) {
		DataSet aptLoc = marmot.getDataSet(APT_LOC);
		String locGeomCol = aptLoc.getGeometryColumn();
		
		DataSet school = marmot.getDataSet(HIGH_SCHOOLS);
		String schoolGeomCol = school.getGeometryColumn();
		
		return marmot.planBuilder("고등학교_주변_아파트_전월세_추출")
					.load(APT_LOC)
					
					// 고등학교 주변 1km 내의 아파트 검색.
					.centroid(locGeomCol)
					.buffer(locGeomCol, 1000, GeomOpOption.OUTPUT("circle"))
					.spatialJoin("circle", HIGH_SCHOOLS,
								String.format("*-{%s},param.{%s,id,name}",
											locGeomCol, schoolGeomCol))
					
					// 고등학교 1km내 위치에 해당하는 아파트 거래 정보를 검색.
					.join("시군구,번지,단지명", APT_LEASE_TRX, "시군구,번지,단지명",
							"the_geom,id,name,param.*", null)
					
					// 고등학교를 기준으로 그룹핑하여 집계한다.
					.groupBy("id")
						.tagWith(schoolGeomCol + ",name")
						.aggregate(COUNT().as("lease_count"))
					.expand1("trade_count:long", "0")
					.project("the_geom,id,name,trade_count,lease_count")
					
					.store(TEMP)
					.build();		
	}
	
	private static DataSet findHighSchool(MarmotRuntime marmot) {
		DataSet ds = marmot.getDataSet(SCHOOLS);
	
		Plan plan = marmot.planBuilder("find_high_school")
							.load(SCHOOLS)
							.filter("type == '고등학교'")
							.store(HIGH_SCHOOLS)
							.build();
		DataSet result = marmot.createDataSet(HIGH_SCHOOLS, ds.getGeometryColumnInfo(),
												plan, DataSetOption.FORCE);
		
		return result;
	}
}
