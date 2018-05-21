package anyang.energe;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
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
public class SplitCadastralElectro {
	private static final String CADASTRAL_ELEC = "tmp/anyang/cadastral_electro";
	private static final String OUTPUT = "tmp/result";
	
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

		DataSet ds = marmot.getDataSet(CADASTRAL_ELEC);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		export(marmot, "11", info);
		export(marmot, "26", info);
		export(marmot, "27", info);
		export(marmot, "28", info);
		export(marmot, "29", info);
		export(marmot, "30", info);
		export(marmot, "31", info);
		export(marmot, "36", info);
		export(marmot, "41", info);
		export(marmot, "42", info);
		export(marmot, "43", info);
		export(marmot, "44", info);
		export(marmot, "45", info);
		export(marmot, "46", info);
		export(marmot, "47", info);
		export(marmot, "48", info);
		export(marmot, "50", info);
		
		marmot.disconnect();
	}
	
	private static final File DIR = new File("/home/kwlee/tmp/cadastral_electro");
	private static void export(PBMarmotClient marmot, String prefix,
								GeometryColumnInfo info) throws IOException, InterruptedException, ExecutionException {
		String filterExpr = String.format("pnu.startsWith('%s')", prefix);
		
		if ( !DIR.exists() ) {
			DIR.mkdir();
		}
		
		Plan plan;
		plan = marmot.planBuilder("전기 사용량 격자 분석")
					.load(CADASTRAL_ELEC)
					.filter(filterExpr)
					.store(OUTPUT)
					.build();
		DataSet ds = marmot.createDataSet(OUTPUT, info, plan, true);
		marmot.writeToShapefile(ds, new File(DIR, prefix + ".shp"), "main",
								StandardCharsets.UTF_8, false, false).get();
		
		System.out.println("created: " + new File(DIR, prefix + ".shp"));
	}
}
