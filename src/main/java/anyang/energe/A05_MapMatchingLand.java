package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.Arrays;
import java.util.stream.Collectors;

import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;

import common.SampleUtils;

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
	private static final FOption<Long> BLOCK_SIZE = FOption.of(UnitUtils.parseByteSize("128mb"));
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

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
		plan = Plan.builder("개별공시지가 매핑")
						.loadHashJoin(BASE, "pnu", INTERM, "pnu", outCols,
										LEFT_OUTER_JOIN(25))
						.update(updateExpr)

						// 공시지가는 평망미터당 지가이므로, 평당액수에 면적을 곱한다.
						.defineColumn("area:double", "ST_Area(the_geom)")
						.update(updatePriceExpr)
						.project("*-{area}")
						
						.store(OUTPUT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		marmot.deleteDataSet(INTERM);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.close();
	}
	
	private static void putSideBySide(PBMarmotClient marmot) {
		RecordSchema outSchema = FStream.of(2012, 2013, 2014, 2015, 2016, 2017)
										.fold(RecordSchema.builder(),
												(b,y) -> b.addColumn("land_"+y, DataType.LONG))
										.build();
		
		Plan plan = Plan.builder("put_side_by_size_land")
						.load(INPUT)
						.project("pnu, 기준년도 as year, 개별공시지가 as usage")
						.expand("tag:string", "tag = 'land_' + year")
						.reduceToSingleRecordByGroup(Group.ofKeys("pnu"), outSchema, "tag", "usage")
						.store(INTERM, FORCE)
						.build();
		marmot.execute(plan);
	}
}
