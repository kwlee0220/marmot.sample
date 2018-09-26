package house.misc;

import static marmot.DataSetOption.FORCE;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.rset.RecordSets;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintCadastral {
	private static final String BUILDINGS = "주소/건물_추진단";
	private static final String DATA01 = "tmp/house/house_cadastral";
	private static final String DATA02 = "tmp/분석결과/연속지적도_주거지역_추출";
	private static final File RESULT01 = new File("/home/kwlee/tmp/result2_1");
	private static final File RESULT02 = new File("/home/kwlee/tmp/result2_2");
	
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
		
		StopWatch total = StopWatch.start();
		StopWatch step;
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		RecordSchema schema = RecordSchema.builder()
											.addColumn("id", DataType.STRING)
											.build();
		List<Record> idList = Files.lines(Paths.get("/home/kwlee/tmp/xxx"))
									.map(str -> {
										Record rec = DefaultRecord.of(schema);
										rec.set(0, str);
										return rec;
									})
									.collect(Collectors.toList());
		RecordSet rset = RecordSets.from(idList);
		marmot.createDataSet("tmp/diff3", rset.getRecordSchema(), FORCE).append(rset);
		
		Plan plan;
		plan = marmot.planBuilder("test")
					.load(BUILDINGS)
					.join("bd_mgt_sn", "tmp/diff3", "id", "*", null)
					.store("tmp/diff_buildings")
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		marmot.createDataSet("tmp/diff_buildings", gcInfo, plan, FORCE);
		
		marmot.disconnect();
	}
}
