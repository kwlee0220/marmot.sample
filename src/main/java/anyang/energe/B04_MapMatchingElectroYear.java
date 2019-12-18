package anyang.energe;

import static marmot.optor.JoinOptions.LEFT_OUTER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B04_MapMatchingElectroYear {
	private static final String CADASTRAL = Globals.CADASTRAL;
	private static final String INPUT = "tmp/anyang/electro" + Globals.YEAR;
	private static final String INTERM = "tmp/anyang/pnu_electro";
	private static final String OUTPUT = "tmp/anyang/map_electro" + Globals.YEAR;

	private static final List<String> COL_NAMES = FStream.rangeClosed(1, 12)
													.map(i -> "month_" + i)
													.toList();
	private static final String PATTERN = "if (%s == null) {%s = 0}";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		putSideBySide(marmot, INTERM);
		
		String rightCols = FStream.from(COL_NAMES).join(",", "right.{", "}");
		String updateExpr = FStream.from(COL_NAMES)
									.map(c -> String.format(PATTERN, c, c))
									.join(" ");
		
		DataSet ds = marmot.getDataSet(CADASTRAL);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		String planName = String.format("%d 전기사용량 연속지적도 매칭", Globals.YEAR);
		Plan plan = Plan.builder(planName)
						.loadHashJoin(CADASTRAL, "pnu", INTERM, "pnu",
									"left.*," + rightCols, LEFT_OUTER_JOIN(17))
						.update(updateExpr)
						.store(OUTPUT, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		marmot.deleteDataSet(INTERM);

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		SampleUtils.printPrefix(result, 10);
		
		marmot.close();
	}

	private static void putSideBySide(PBMarmotClient marmot, String outDsId) {
		RecordSchema outSchema = FStream.from(COL_NAMES)
										.foldLeft(RecordSchema.builder(),
												(b,cn) -> b.addColumn(cn, DataType.LONG))
										.build();
		
		Plan plan = Plan.builder("put_side_by_side_electro")
						.load(INPUT)
						.expand("tag:string", "tag = 'month_' + month")
						.reduceToSingleRecordByGroup(Group.ofKeys("pnu"), outSchema, "tag", "usage")
						.store(outDsId, FORCE)
						.build();
		marmot.execute(plan);
	}
}
