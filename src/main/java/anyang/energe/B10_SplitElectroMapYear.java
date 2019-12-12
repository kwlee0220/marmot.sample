package anyang.energe;

import static marmot.optor.StoreDataSetOptions.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class B10_SplitElectroMapYear {
	private static final String INPUT = "tmp/anyang/map_electro" + Globals.YEAR;
	private static final String OUTPUT = "tmp/anyang/map_electro" + Globals.YEAR + "_splits";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo info = marmot.getDataSet(INPUT).getGeometryColumnInfo();

		String planName = String.format("%d 전기사용량 연속지적도 매칭 분할", Globals.YEAR);
		Plan plan = marmot.planBuilder(planName)
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
