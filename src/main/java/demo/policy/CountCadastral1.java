package demo.policy;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
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
public class CountCadastral1 {
	private static final String PARAM = "구역/행정동코드";
	private static final String INPUT = "구역/연속지적도_2017";
	private static final String RESULT = "tmp/count_cadastral";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("step07 ");
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
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("행정도별 필지수 계산")
						.load(INPUT)
						.spatialJoin("the_geom", PARAM, INTERSECTS, "pnu,param.hcode")
						.groupBy("hcode")
//							.workerCount(17)
							.aggregate(COUNT())
//						.spatialAggregateJoin("the_geom", PARAM, INTERSECTS, COUNT())
//						.shard(1)
						.store(RESULT)
						.build();
		DataSet result = marmot.createDataSet(RESULT, plan, true);
//		result.cluster();
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}