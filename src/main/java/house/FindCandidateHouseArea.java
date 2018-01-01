package house;

import org.apache.log4j.PropertyConfigurator;

import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindCandidateHouseArea {
	private static final String LAND_USAGE = "토지/용도지역지구_추진단";
	private static final String CADASTRAL = "구역/연속지적도_추진단";
	private static final String BUILDINGS = "주소/건물_추진단";
	private static final String REGISTRY = "건물/건축물대장/총괄표제부_fixed";
	private static final String HOUSE_AREA = "tmp/house/house_area";
	private static final String HOUSE_CADASTRAL = "tmp/house/house_cadastral";
	private static final String REG_BUILDINGS = "tmp/house/registered_buildings";
	private static final String RESULT = "tmp/house/candiate_area";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}
		
		StopWatch total = StopWatch.start();

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		// 용도지구에서 주거 지역 추출
		S01_FindHouseArea.process(marmot, LAND_USAGE, HOUSE_AREA);
		
		// 전국 지적도에서 주거지적 추출
		S02_FindHouseCadastral.process(marmot, CADASTRAL, HOUSE_AREA, HOUSE_CADASTRAL);
		
		// 전국 건물 중에서 총괄표제부 보유한 건물 추출
		S03_FindRegistreredBuildings.process(marmot, BUILDINGS, REGISTRY, REG_BUILDINGS);

		// 전국 주거가능 지적 영역 중에서 총괄표제부를 보유한 건물 영역을 제외한 영역 추출
		S04_FindHouseCandidates.process(marmot, HOUSE_CADASTRAL, REG_BUILDINGS, RESULT);
		
		System.out.printf("주택건설가능 택지 분석 완료, elapsed: %s%n",
							total.stopAndGetElpasedTimeString());
		marmot.disconnect();
	}
}
