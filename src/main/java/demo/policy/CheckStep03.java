package demo.policy;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CheckStep03 {
//	private static final String INPUT = "tmp/10min/step03";
//	private static final File RESULT = new File("/home/kwlee/tmp/10min/step03_2");
	private static final String INPUT = "구역/연속지적도_2017";
//	private static final String INPUT = Step03.INPUT;
	private static final String RESULT = "tmp/error";
	
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

		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("test")
							.load(INPUT)
							.filter("pnu == '2911010800101610001'"
									+ "|| pnu == '2911010800101540006'"
									+ "|| pnu == '2911010800101610002'")
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, info, plan, DataSetOption.FORCE);
	}
}
