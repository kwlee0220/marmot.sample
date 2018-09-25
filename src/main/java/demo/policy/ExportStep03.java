package demo.policy;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportStep03 {
	private static final String INPUT1 = "tmp/10min/step03";
	private static final File RESULT1 = new File("/home/kwlee/tmp/10min/step03");
	private static final String INPUT2 = "분석결과/10분정책/step03";
	private static final File RESULT2 = new File("/home/kwlee/tmp/10min/step03_g");
	
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
		
		DataSet ds = marmot.getDataSet(INPUT1);
		try ( RecordSet rset = ds.read();
				PrintWriter pw = new PrintWriter(new FileWriter(RESULT1)) ) {
			rset.forEach(rec -> {
//				double area = rec.getGeometry("the_geom").getArea();
				String pnu = rec.getString("pnu");
				pw.printf("%s%n", pnu);
			});
		}
		
		DataSet ds2 = marmot.getDataSet(INPUT2);
		try ( RecordSet rset = ds2.read();
				PrintWriter pw = new PrintWriter(new FileWriter(RESULT2)) ) {
			rset.forEach(rec -> {
//				double area = rec.getGeometry("the_geom").getArea();
				String pnu = rec.getString("pnu");
				pw.printf("%s%n", pnu);
			});
		}
	}
}
