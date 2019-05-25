package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import io.vavr.control.Option;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
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
public class A05_MapMatchingLand {
	private static final String INPUT = Globals.LAND_PRICES;
	private static final String BASE = Globals.LAND_PRICES_2017;
	private static final String INTERM = "tmp/anyang/land_side_by_side";
	private static final String OUTPUT = "tmp/anyang/map_land";
	private static final String PATTERN = "if (land_%d == null) {land_%d = 0}";
	private static final String PATTERN2 = "land_%d *= area;";
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

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		putSideBySide(marmot);

		int[] years = { 2012, 2013, 2014, 2015, 2016 };
		
		DataSet base = marmot.getDataSet(BASE);
		GeometryColumnInfo gcInfo = base.getGeometryColumnInfo();
		
		String rightCols = Arrays.stream(years)
								.mapToObj(i -> "land_" + i)
								.collect(Collectors.joining(",", "right.{", "}"));
		String outCols = "left.{the_geom, pnu}," + rightCols
						+ ",left.{개별공시지가 as land_2017}";
		String updateExpr = FStream.of(years)
									.map(year -> String.format(PATTERN, year, year))
									.join(" ");
		
		int[] fullYears = { 2012, 2013, 2014, 2015, 2016, 2017 };
		String updatePriceExpr = FStream.of(fullYears)
										.map(year -> String.format(PATTERN2, year))
										.join(" ");

		Plan plan;
		plan = marmot.planBuilder("개별공시지가 매핑")
						.loadHashJoinFile(BASE, "pnu", INTERM, "pnu", outCols,
										LEFT_OUTER_JOIN(25))
						.update(updateExpr)

						// 공시지가는 평망미터당 지가이므로, 평당액수에 면적을 곱한다.
						.defineColumn("area:double", "ST_Area(the_geom)")
						.update(updatePriceExpr)
						.project("*-{area}")
						
						.store(OUTPUT)
						.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		marmot.deleteDataSet(INTERM);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.disconnect();
	}
	
	private static void putSideBySide(PBMarmotClient marmot) {
		RecordSchema outSchema = FStream.of(2012, 2013, 2014, 2015, 2016, 2017)
										.foldLeft(RecordSchema.builder(),
												(b,y) -> b.addColumn("land_"+y, DataType.LONG))
										.build();
		
		Plan plan = marmot.planBuilder("put_side_by_size_land")
						.load(INPUT)
						.project("pnu, 기준년도 as year, 개별공시지가 as usage")
						.expand("tag:string", "tag = 'land_' + year")
						.groupBy("pnu")
							.putSideBySide(outSchema, "usage", "tag")
						.store(INTERM)
						.build();
		marmot.createDataSet(INTERM, plan, StoreDataSetOptions.create().force(true));
	}
}
