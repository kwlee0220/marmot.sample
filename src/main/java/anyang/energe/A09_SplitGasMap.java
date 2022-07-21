package anyang.energe;

import static marmot.optor.StoreDataSetOptions.GEOMETRY;

import utils.StopWatch;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class A09_SplitGasMap {
	private static final String INPUT = "tmp/anyang/map_gas";
	private static final String OUTPUT = "tmp/anyang/map_gas_splits";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo info = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		Plan plan = Plan.builder("2017 가스사용량 연속지적도 매칭 분할")
						.load(INPUT)
						.defineColumn("sido:string", "pnu.substring(0, 2)")
						.storeByGroup(Group.ofKeys("sido"), OUTPUT, GEOMETRY(info))
						.build();
		
		marmot.deleteDir(OUTPUT);
		marmot.execute(plan);
		watch.stop();

		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		marmot.close();
	}
}
