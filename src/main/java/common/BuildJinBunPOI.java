package common;

import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildJinBunPOI {
	private static final String JIBUN = "주소/지번";
	private static final String ADDR = "주소/주소";
	private static final String BUILD_POI = "주소/건물POI";
	private static final String RESULT = "주소/지번POI";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		Plan plan;
		DataSet result;
		RecordSchema schema;

		String tempDs = "tmp/" + UUID.randomUUID().toString();
		plan = marmot.planBuilder("distinct_jibun")
					.load(JIBUN)
					.distinct("건물관리번호", 11) 
					.store(tempDs)
					.build();
		result = marmot.createDataSet(tempDs, plan, StoreDataSetOptions.create().force(true));

		try {
			DataSet ds = marmot.getDataSet(BUILD_POI);
			String geomCol = ds.getGeometryColumn();
			
			plan = marmot.planBuilder("build_jibun_poi")
							.load(BUILD_POI)
							.project(geomCol + ",도로명코드,건물본번,건물부번,지하여부,법정동코드")
							.hashJoin("도로명코드,건물본번,건물부번,지하여부",
									ADDR, "도로명코드,건물본번,건물부번,지하여부",
									geomCol + ",param.{건물관리번호}",
									new JoinOptions().workerCount(23))
							.hashJoin("건물관리번호", tempDs, "건물관리번호",
									"*,param.{법정동코드,지번본번,지번부번,산여부}", null)
							.distinct("건물관리번호,법정동코드,지번본번,지번부번,산여부")
							.store(RESULT)
							.build();
			GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
			result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
			System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
			
			SampleUtils.printPrefix(result, 5);
		}
		finally {
			marmot.deleteDataSet(tempDs);
		}
	}
}
