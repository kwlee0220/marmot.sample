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
public class S06_MatchLandPrices {
	private static final String INPUT = Globals.LAND_PRICES;
	private static final String INTERM = "tmp/anyang/pnu_land";
	private static final String BASE = Globals.LAND_PRICES_2017;
	private static final String TEMP = "tmp/anyang/temp";
	private static final String OUTPUT = "tmp/anyang/cadastral_land";
	private static final String PATTERN = "if (land_%d == null) {land_%d = 0}";
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

		int[] years = { 2012, 2013, 2014, 2015, 2016 };
		
		DataSet base = marmot.getDataSet(BASE);
		GeometryColumnInfo info = base.getGeometryColumnInfo();
		
		String rightCols = Arrays.stream(years)
								.mapToObj(i -> "land_" + i)
								.collect(Collectors.joining(",", "right.{", "}"));
		String outCols = "left.{the_geom, 고유번호 as pnu}," + rightCols
						+ ",left.{개별공시지가 as land_2017}";
		String updateExpr = FStream.of(years)
									.map(year -> String.format(PATTERN, year, year))
									.join(" ");

		Plan plan;
		plan = marmot.planBuilder("개별공시지가 매핑")
						.loadEquiJoin(BASE, "고유번호", INTERM, "pnu", outCols,
										LEFT_OUTER_JOIN(25))
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
		RecordSchema outSchema = FStream.of(2012, 2013, 2014, 2015, 2016, 2017)
										.foldLeft(RecordSchema.builder(),
												(b,y) -> b.addColumn("land_"+y, DataType.LONG))
										.build();
		
		Plan plan = marmot.planBuilder("put_side_by_size_land")
						.load(INPUT)
						.project("고유번호 as pnu, 기준년도 as year, 개별공시지가 as usage")
						.expand("tag:string", "tag = 'land_' + year")
						.groupBy("pnu")
							.putSideBySide(outSchema, "usage", "tag")
						.store(INTERM)
						.build();
		marmot.createDataSet(INTERM, plan, true);
	}
	
	private static DataSet match(PBMarmotClient marmot, int year) {
		extractToYear(marmot, year);
		
		String output = INTERM + "_next";
		DataSet result = match(marmot, INTERM, output, year);
		
		marmot.deleteDataSet(TEMP);
		marmot.deleteDataSet(INTERM);
		marmot.renameDataSet(result.getId(), INTERM);
		
		return marmot.getDataSet(INTERM);
	}
	
	private static void extractToYear(PBMarmotClient marmot, int year) {
		Plan plan;
		String planName = String.format("%s년도 개별공시지가 추출", year);
		String filter = String.format("기준년도 == '%s'", year);
		
		plan = marmot.planBuilder(planName)
					.load(INPUT)
					.filter(filter)
					.project("고유번호 as pnu, 개별공시지가 as price")
					.shard(1)
					.store(TEMP)
					.build();
		marmot.createDataSet(TEMP, plan, true);
	}
	
	private static DataSet match(PBMarmotClient marmot, String input, String output,
								int year) {
		String planName = String.format("%s년도 개별공시지가 매핑", year);
		String paramCol = String.format("land_%s", year);
		String outCols = String.format("left.*, right.{price as %s}", paramCol);
		String updateExpr = String.format("if (%s == null) { %s = 0; }", paramCol, paramCol, paramCol);
		
		Plan plan;
		plan = marmot.planBuilder(planName)
					.loadEquiJoin(input, "pnu", TEMP, "pnu", outCols, LEFT_OUTER_JOIN(11))
					.update(updateExpr)
					.store(output)
					.build();
		return marmot.createDataSet(output, plan, true);
	}
}
