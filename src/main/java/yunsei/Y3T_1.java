package yunsei;

import static marmot.plan.SpatialJoinOption.NEGATED;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.geo.LISAWeight;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.DimensionDouble;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Y3T_1 {
	private static final String POPULATION = "연세대/유동인구_부산";
	private static final String PUBLIC_CARE = "POI/사회보장시설";
	private static final String TEMP_ELDERLY_CARES = "tmp/elderly_cares";
	private static final String TEMP_POP = "tmp/pop";
	private static final String RESULT = "tmp/result";
	
	private static final DimensionDouble CELL_SIZE = new DimensionDouble(1000,1000);
	private static final int NWORKERS = 25;
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
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
		DataSet result;
		
		findElderlyCares(marmot, TEMP_ELDERLY_CARES);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
		
		DataSet pop = marmot.getDataSet(POPULATION);
		GeometryColumnInfo gcInfo = pop.getGeometryColumnInfo();
		String geomCol = pop.getGeometryColumn();
		
		plan = marmot.planBuilder("")
					.load(POPULATION)
					.spatialSemiJoin(pop.getGeometryColumn(), TEMP_ELDERLY_CARES)
					.update("refl70 = 0")
					.project(geomCol + ",refl70,point_x,point_y")
					.store(TEMP_POP)
					.build();
		result = marmot.createDataSet(TEMP_POP, gcInfo, plan, DataSetOption.FORCE);
		System.out.println("elapsed: " + watch.getElapsedMillisString());

		plan = marmot.planBuilder("")
					.load(POPULATION)
					.spatialSemiJoin(pop.getGeometryColumn(), TEMP_ELDERLY_CARES, NEGATED)
					.project(geomCol + ",refl70,point_x,point_y")
					.store(TEMP_POP)
					.build();
		marmot.execute(plan);
		result.cluster();
		System.out.println("elapsed: " + watch.getElapsedMillisString());
		
		plan = marmot.planBuilder("핫 스팟 분석")
					.loadGetisOrdGi(TEMP_POP, "refl70", 500, LISAWeight.FIXED_DISTANCE_BAND)
					.store(RESULT)
					.build();
		result = marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);
		
		System.out.println("done, elapsed=" + watch.stopAndGetElpasedTimeString());
	}
	
	private static DataSet findElderlyCares(MarmotRuntime marmot, String outputDsId) {
		Plan plan;
		
		DataSet ds = marmot.getDataSet(PUBLIC_CARE);
		plan = marmot.planBuilder("노인복지시설 검색 후 500m 버퍼")
					.load(PUBLIC_CARE)
					.filter("시설종류코드=='5040200000000'")
					.buffer(ds.getGeometryColumn(), 500)
					.store(outputDsId)
					.build();
		DataSet result = marmot.createDataSet(outputDsId, ds.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		
		return result;
	}
	
//	private static DataSet tagGeomToPopulation(MarmotClient marmot, String outputDsId) {
//		Plan plan;
//		
//		String filterExpr = "age_intl==75 || age_intl==80"
//						  + "|| age_intl==85 || age_intl==90"
//						  + "|| age_intl==95 || age_intl==100"
//						  + "|| age_intl==105";
//		
//		DataSet ds = marmot.getDataSet(SGG);
//		plan = marmot.planBuilder("geo_population")
//					.load(POPULATION)
//					.filter(filterExpr)
//					.groupBy("sig_cd")
//						.taggedKeyColumns("sig_nm")
//						.aggregate(SUM("pop_tot").as("pop_tot"))
//					.join("sig_cd", SGG, "sig_cd", "*,param.the_geom", null)
//					.store(outputDsId)
//					.build();
//		marmot.deleteDataSet(outputDsId);
//		return marmot.createDataSet(outputDsId, ds.getGeometryColumn(), ds.getSRID(), plan);
//	}
}
