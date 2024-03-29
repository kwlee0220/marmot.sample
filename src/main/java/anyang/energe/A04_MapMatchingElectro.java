package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.Arrays;
import java.util.stream.Collectors;

import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A04_MapMatchingElectro {
	private static final String CADASTRAL = Globals.CADASTRAL;
	private static final String INPUT = "tmp/anyang/electro_by_year";
	private static final String INTERM = "tmp/anyang/electro_side_by_side";
	private static final String OUTPUT = "tmp/anyang/map_electro";
	private static final String PATTERN = "if (electro_%d == null) {electro_%d = 0}";
	private static final FOption<Long> BLOCK_SIZE = FOption.of(UnitUtils.parseByteSize("128mb"));
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		putSideBySide(marmot);

		int[] years = { 2011, 2012, 2013, 2014, 2015, 2016, 2017 };
		String rightCols = Arrays.stream(years)
								.mapToObj(i -> "electro_" + i)
								.collect(Collectors.joining(",", "right.{", "}"));
		String updateExpr = FStream.of(years)
									.map(year -> String.format(PATTERN, year, year))
									.join(" ");
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = Plan.builder("연속지적도 매칭")
						.loadHashJoin(CADASTRAL, "pnu", INTERM, "pnu",
										"left.*," + rightCols, LEFT_OUTER_JOIN(17))
						.update(updateExpr)
						.store(OUTPUT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		marmot.deleteDataSet(INTERM);
		marmot.deleteDataSet(INPUT);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.close();
	}
	
	private static void putSideBySide(PBMarmotClient marmot) {
		RecordSchema outSchema = FStream.of(2011, 2012, 2013, 2014, 2015, 2016, 2017)
										.fold(RecordSchema.builder(),
												(b,y) -> b.addColumn("electro_"+y, DataType.LONG))
										.build();
		
		Plan plan = Plan.builder("put_side_by_size_electro")
						.load(INPUT)
						.expand("tag:string", "tag = 'electro_' + year")
						.reduceToSingleRecordByGroup(Group.ofKeys("pnu"), outSchema, "tag", "usage")
						.store(INTERM, FORCE)
						.build();
		marmot.execute(plan);
	}
}
