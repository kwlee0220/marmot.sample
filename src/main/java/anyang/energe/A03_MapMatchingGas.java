package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;

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
public class A03_MapMatchingGas {
	private static final String CADASTRAL = Globals.CADASTRAL;
	private static final String INPUT = "tmp/anyang/gas_by_year";
	private static final String INTERM = "tmp/anyang/gas_side_by_side";
	private static final String OUTPUT = "tmp/anyang/map_gas";
	private static final String PATTERN = "if (gas_%d == null) {gas_%d = 0}";
	private static final int[] YEARS = {2011, 2012, 2013, 2014, 2015, 2016, 2017};
	private static final List<String> COL_NAMES = FStream.of(YEARS).map(i -> "gas_" + i).toList();
	private static final FOption<Long> BLOCK_SIZE = FOption.of(UnitUtils.parseByteSize("128mb"));
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		putSideBySide(marmot);
		
		String rightCols = FStream.from(COL_NAMES).join(",", "right.{", "}");
		String updateExpr = FStream.of(YEARS)
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
		RecordSchema outSchema = FStream.from(COL_NAMES)
										.fold(RecordSchema.builder(),
												(b,cn) -> b.addColumn(cn, DataType.LONG))
										.build();
		
		Plan plan = Plan.builder("put_side_by_size_gas")
						.load(INPUT)
						.expand("tag:string", "tag = 'gas_' + year")
						.reduceToSingleRecordByGroup(Group.ofKeys("pnu"), outSchema, "tag", "usage")
						.store(INTERM, FORCE)
						.build();
		marmot.execute(plan);
	}
}
