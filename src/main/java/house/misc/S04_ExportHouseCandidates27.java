package house.misc;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S04_ExportHouseCandidates27 {
	private static final String CANDIDATE_AREA = "tmp/house/house_candidates";
	private static final File SHP_FILE = new File("/home/kwlee/tmp/house_candidates_27.shp");
	
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
		
		DataSet input = marmot.getDataSet(CANDIDATE_AREA);
		
		Plan plan;
		plan = marmot.planBuilder("export01")
					.load(CANDIDATE_AREA)
					.filter("pnu.startsWith('27')")
					.project("the_geom,pnu")
					.store(CANDIDATE_AREA + "_27")
					.build();
		DataSet ds = marmot.createDataSet(CANDIDATE_AREA + "_27", input.getGeometryColumnInfo(), plan, DataSetOption.FORCE);

		Charset charset = Charset.forName("UTF-8");
		marmot.writeToShapefile(ds, SHP_FILE, charset).get();
	}
}
