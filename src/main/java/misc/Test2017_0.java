package misc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
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
public class Test2017_0 {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final List<String> BLD_CODES = Arrays.asList(
		"03101", "03102", "03103", "03104", "03105", "03107", "03108", "03109",
		"04010", "04301", "04401", "05201", "05202", "05403", "05404",
		"05501", "05502", "05503", "05504", "05505", "05506", "05599",
		"05601", "05602", "05603", "05699", "06202", "06203", "06204",
		"06205", "06303", "06305", "07101", "07102", "07103", "07104", "07107",
		"08001", "08002", "08003", "08004", "08005", 
		"08101", "08102", "08103", "08104", "08105", "08106", "08199", 
		"08201", "08202", "08203", "08204", "08299", 
		"08300", "08400", "08500", "08601", "08602", "08603", "08699", 
		"09001", "09002", "10101", "10102", "10103", "10199",
		"10204", "10299", "19005", "21001", "21002", "21003"
	);
	
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

		DataSet input = marmot.getDataSet(ADDR_BLD);
		String srid = input.getGeometryColumnInfo().srid();
		
		String initExpr = BLD_CODES.stream()
									.map(cd -> "\"" + cd + "\"")
									.collect(Collectors.joining(",", "[", "]"));
		initExpr = "$codes = Sets.newHashSet(); $codes.addAll(" + initExpr + ")";
		
		Plan plan = marmot.planBuilder("get_biz_grid")
								.load(ADDR_BLD)
								.filter(initExpr, "$codes.contains(건물용도코드)")
								.project("the_geom,건물관리번호")
								.store(ADDR_BLD_UTILS)
								.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		DataSet result = marmot.createDataSet(ADDR_BLD_UTILS, gcInfo, plan, DataSetOption.FORCE);
		result.cluster();
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
