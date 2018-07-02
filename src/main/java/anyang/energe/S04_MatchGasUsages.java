package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S04_MatchGasUsages {
	private static final String CADASTRAL = "tmp/anyang/cadastral";
	private static final String INPUT = "tmp/anyang/gas_year";
	private static final String INTERM = "tmp/anyang/pnu_gas";
	private static final String OUTPUT = "tmp/anyang/cadastral_gas";
	private static final String PATTERN = "if (gas_%d == null) {gas_%d = 0}";
	private static final int[] YEARS = {2011, 2012, 2013, 2014, 2015, 2016, 2017};
	private static final List<String> COL_NAMES = FStream.of(YEARS).map(i -> "gas_" + i).toList();
	private static final Option<Long> BLOCK_SIZE = Option.some(UnitUtils.parseByteSize("128mb"));
	
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

		putSideBySide(marmot);
		
		String rightCols = FStream.of(COL_NAMES).join(",", "right.{", "}");
		String updateExpr = FStream.of(YEARS)
									.map(year -> String.format(PATTERN, year, year))
									.join(" ");
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("연속지적도 매칭")
						.loadEquiJoin(CADASTRAL, "pnu", INTERM, "pnu",
										"left.*," + rightCols, LEFT_OUTER_JOIN(17))
						.update(updateExpr)
						.store(OUTPUT)
						.build();
		DataSet result = marmot.createDataSet(OUTPUT, info, plan, true);
		marmot.deleteDataSet(INTERM);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
	
	private static void putSideBySide(PBMarmotClient marmot) {
		RecordSchema outSchema = FStream.of(COL_NAMES)
										.foldLeft(RecordSchema.builder(),
												(b,cn) -> b.addColumn(cn, DataType.LONG))
										.build();
		
		Plan plan = marmot.planBuilder("put_side_by_size_gas")
						.load(INPUT)
						.expand("tag:string", "tag = 'gas_' + year")
						.groupBy("pnu")
							.putSideBySide(outSchema, "usage", "tag")
						.store(INTERM)
						.build();
		marmot.createDataSet(INTERM, plan, true);
	}
}