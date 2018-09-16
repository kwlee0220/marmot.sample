package house.misc;

import java.io.File;
import java.nio.charset.Charset;

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
public class ExporBuildings27 {
	private static final String BUILDINGS = "주소/건물_추진단";
	private static final String RESULT = "tmp/house/buildings_27";
	private static final File SHP_FILE = new File("/home/kwlee/tmp/buildings_27.shp");
	
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
					.load(BUILDINGS)
					.filter("sig_cd.startsWith('27')")
					.project("the_geom,bd_mgt_sn")
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);

		Charset charset = Charset.forName("UTF-8");
		marmot.writeToShapefile(result, SHP_FILE, charset).get();
		
		marmot.disconnect();
	}
}
