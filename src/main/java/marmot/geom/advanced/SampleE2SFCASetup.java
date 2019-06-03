package marmot.geom.advanced;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleE2SFCASetup {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String SGG = "구역/시군구";
	private static final String RESULT = "주민/유동인구/강남구/시간대/2015";
	
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

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		Plan plan;

		StopWatch watch = StopWatch.start();
		Geometry gangnaum = getGangnamGu(marmot);
		
		DataSet flowPop = marmot.getDataSet(FLOW_POP);
		plan = marmot.planBuilder("강남구 영역 유동인구 정보 추출")
						.query(FLOW_POP, gangnaum)
						.expand("year:int", "year = std_ym.substring(0,4)")
						.aggregateByGroup(Group.ofKeys("block_cd,year").tags("the_geom"),
											AVG("avg_08tmst").as("avg_08tmst"),
											AVG("avg_15tmst").as("avg_15tmst"))
						.project("*-{year}")
						.build();
		GeometryColumnInfo gcInfo = flowPop.getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static Geometry getGangnamGu(MarmotRuntime marmot) {
		Plan plan;
		plan = marmot.planBuilder("강남구 추출")
					.load(SGG)
					.filter("sig_cd.startsWith('11') && sig_kor_nm == '강남구'")
					.project("the_geom")
					.build();
		return marmot.executeLocally(plan).toList().get(0).getGeometry("the_geom");
	}
}
