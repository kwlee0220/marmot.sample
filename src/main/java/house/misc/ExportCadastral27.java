package house.misc;

import java.io.File;

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
public class ExportCadastral27 {
	private static final String CADASTRAL = "구역/연속지적도_추진단";
	private static final String RESULT = "tmp/house/cadastral_272";
	private static final File SHP_FILE = new File("/home/kwlee/tmp/cadastral_27.shp");
	
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
		
		Plan plan;
		plan = marmot.planBuilder("test")
					.load(CADASTRAL)
					.filter("pnu.startsWith('2726010')")
//					.filter("pnu == '2726010300100960413'")
					.project("the_geom,pnu")
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);

//		Charset charset = Charset.forName("UTF-8");
//		marmot.writeToShapefile(result, SHP_FILE, "main", charset, false, false).get();
		
		marmot.disconnect();
	}
}
