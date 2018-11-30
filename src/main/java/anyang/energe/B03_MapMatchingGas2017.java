package anyang.energe;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;
import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B03_MapMatchingGas2017 {
	private static final String CADASTRAL = Globals.CADASTRAL;
	private static final String INPUT = "tmp/anyang/gas2017";
	private static final String INTERM = "tmp/anyang/pnu_gas";
	private static final String OUTPUT = "tmp/anyang/map_gas2017";

	private static final List<String> COL_NAMES = FStream.rangeClosed(1, 12)
													.map(i -> "month_" + i)
													.toList();
	private static final String PATTERN = "if (%s == null) {%s = 0}";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		putSideBySide(marmot, INTERM);
		
		String rightCols = FStream.of(COL_NAMES).join(",", "right.{", "}");
		String updateExpr = FStream.of(COL_NAMES)
									.map(c -> String.format(PATTERN, c, c))
									.join(" ");
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("2017 가스사용량 연속지적도 매칭")
						.loadHashJoin(CADASTRAL, "pnu", INTERM, "pnu",
									"left.*," + rightCols, LEFT_OUTER_JOIN(17))
						.update(updateExpr)
						.store(OUTPUT)
						.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(gcInfo), FORCE);
		marmot.deleteDataSet(INTERM);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}

	private static void putSideBySide(PBMarmotClient marmot, String outDsId) {
		RecordSchema outSchema = FStream.of(COL_NAMES)
										.foldLeft(RecordSchema.builder(),
												(b,cn) -> b.addColumn(cn, DataType.LONG))
										.build();
		
		Plan plan = marmot.planBuilder("put_side_by_size_gas")
						.load(INPUT)
						.expand("tag:string", "tag = 'month_' + month")
						.groupBy("pnu")
							.putSideBySide(outSchema, "usage", "tag")
						.store(outDsId)
						.build();
		marmot.createDataSet(outDsId, plan, DataSetOption.FORCE);
	}
}
