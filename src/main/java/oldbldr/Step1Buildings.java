package oldbldr;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1Buildings {
	private static final String BUILDINGS = "건물/통합정보";
	private static final String EMD = "구역/읍면동";
	private static final String RESULT = "tmp/building_age";
	
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
		DataSet emd = marmot.getDataSet(EMD);
		String geomCol = emd.getGeometryColumn();
		
		String schemaStr = "old:byte,be5:byte";
		String init = "$now = ST_DateNow();";
		String trans = "$date = (사용승인일자 != null && 사용승인일자.length() >= 8) "
								+ "? ST_DateParse(사용승인일자,'yyyyMMdd') : null;"
						+ "$period = ($date != null) ? ST_DateDaysBetween($date,$now) : -1;"
						+ "$age = $period/365L;"
						+ "old = $age >= 20 ? 1 : 0;"
						+ "be5 = $age >= 5 ? 1 : 0;";
		
		plan = marmot.planBuilder("행정구역당 20년 이상된 건물 집계")
					.load(BUILDINGS)
					.expand(schemaStr, init, trans)
					.spatialJoin("the_geom", EMD, INTERSECTS,
								"원천도형ID,old,be5,param.{the_geom,emd_cd,emd_kor_nm as emd_nm}")
					.groupBy("emd_cd")
						.tagWith(geomCol + ",emd_nm")
						.workerCount(1)
						.aggregate(SUM("old").as("old_cnt"), SUM("be5").as("be5_cnt"),
									COUNT().as("bld_cnt"))
					.expand("old_ratio:double", "old_ratio = (double)old_cnt/bld_cnt")
					.store(RESULT)
					.build();
		DataSet result = marmot.createDataSet(RESULT, emd.getGeometryColumnInfo(), plan, true);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
