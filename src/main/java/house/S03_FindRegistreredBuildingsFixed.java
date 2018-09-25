package house;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.plan.LoadOption;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_FindRegistreredBuildingsFixed {
	private static final String BUILDINGS = "주소/건물_추진단";
	private static final String REGISTRY = "건물/건축물대장/총괄표제부";
	private static final String RESULT = "tmp/house/registered_buildings";
	
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
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		// 전국 건물 중에서 총괄표제부 보유한 건물 추출
		process(marmot, BUILDINGS, REGISTRY, RESULT);
		
		marmot.disconnect();
	}
	
	static final DataSet process(MarmotRuntime marmot, String buildings, String registry,
									String result)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet input = marmot.getDataSet(buildings);
		String geomCol = input.getGeometryColumn();

		Plan plan = marmot.planBuilder("총괄표제부 보유 건물 추출")
						.load(registry, LoadOption.SPLIT_COUNT(8))
						.knnJoin(geomCol, buildings, 10, 1, "param.*")
						.store(result)
						.build();
		DataSet ds = marmot.createDataSet(result, input.getGeometryColumnInfo(), plan, DataSetOption.FORCE);
		ds.cluster();
		elapsed.stop();
		
		System.out.printf("총괄표제부 보유 건물 추출 완료, count=%d, elapsed=%s%n",
							ds.getRecordCount(), elapsed.getElapsedMillisString());
		
		return ds;
	}
}
