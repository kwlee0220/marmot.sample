package house.misc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_ExportCompare {
	private static final String DATA01 = "tmp/house/house_cadastral";
	private static final String DATA01_IDX = "tmp/house/house_cadastralIdx";
	private static final String DATA02 = "tmp/분석결과/연속지적도_주거지역_추출";
	private static final File RESULT01 = new File("/home/kwlee/tmp/house2_1");
	private static final File RESULT01_IDX = new File("/home/kwlee/tmp/house2_1_idx");
	private static final File RESULT02 = new File("/home/kwlee/tmp/house2_2");
	
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
		
		exportData(marmot, DATA01, RESULT01);
		exportData(marmot, DATA01_IDX, RESULT01_IDX);
//		exportData(marmot, DATA02, RESULT02);
		
		marmot.disconnect();
	}
	
	private static void exportData(MarmotRuntime marmot, String input, File output)
		throws FileNotFoundException {
		Plan plan;
		plan = marmot.planBuilder("export01")
					.load(input)
					.expand1("area:double", "ST_Area(the_geom)")
					.project("pnu,area")
					.sort("pnu:A,area:A")
					.store("tmp/result")
					.build();
		DataSet ds = marmot.createDataSet("tmp/result", plan, DataSetOption.FORCE);
		
		try ( PrintStream ps = new PrintStream(output);
			RecordSet rset = ds.read() ) {
			rset.forEach(r -> {
				String pnu = r.getString("pnu");
				double area = r.getDouble("area", -1);
				
				ps.printf("%s,%.2f%n", pnu, area);
			});
			ps.flush();
		}
	}
}
