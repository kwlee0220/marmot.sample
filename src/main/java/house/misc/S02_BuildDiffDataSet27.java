package house.misc;

import static marmot.DataSetOption.FORCE;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
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

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_BuildDiffDataSet27 {
	private static final String HOUSE_CADASTRAL = "tmp/house/house_cadastral";
	private static final String DIFF_ID_LIST = "tmp/diff_2";
	private static final File SHP_FILE = new File("/home/kwlee/tmp/diff_cadastral_27.shp");
	
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
		
		RecordSchema schema = RecordSchema.builder()
											.addColumn("pnu", DataType.STRING)
											.addColumn("area", DataType.DOUBLE)
											.build();
		List<Record> idList = Files.lines(Paths.get("/home/kwlee/tmp/diff_2"))
									.map(str -> {
										String[] parts = str.split(",");
										
										Record rec = DefaultRecord.of(schema);
										rec.set(0, parts[0]);
										rec.set(1, Double.parseDouble(parts[1]));
										return rec;
									})
									.collect(Collectors.toList());
		RecordSet idSet = RecordSets.from(idList);
		marmot.createDataSet(DIFF_ID_LIST, idSet.getRecordSchema(), FORCE).append(idSet);
		
		Plan plan;
		plan = marmot.planBuilder("test")
					.load(HOUSE_CADASTRAL)
					.join("pnu", DIFF_ID_LIST, "pnu", "*,param.area", null)
					.filter("pnu.startsWith('27')")
					.filter("Math.abs(ST_Area(the_geom) - area) < 1")
					.store("tmp/diff_cadastral")
					.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet("tmp/diff_cadastral", gcInfo, plan, FORCE);
		marmot.deleteDataSet(DIFF_ID_LIST);

		Charset charset = Charset.forName("UTF-8");
		marmot.writeToShapefile(result, SHP_FILE, charset).get();
		
		
		marmot.disconnect();
	}
}
