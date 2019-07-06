package appls;

import static marmot.StoreDataSetOptions.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PreProcessAgePop {
	private static final String INPUT = "주민/성연령별인구";
	private static final String RESULT = "분석결과/5차년도_통합시연/연령대별_인구";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(INPUT).getGeometryColumnInfo();
		
		plan = marmot.planBuilder("pre_process_age_pop")
					.load(INPUT)
					.defineColumn("base_year:int")
					.defineColumn("age_intvl:int", "(item_name.substring(7) / 10) * 10")
					.aggregateByGroup(Group.ofKeys("tot_oa_cd,base_year,age_intvl").tags("the_geom"),
									AggregateFunction.SUM("value").as("total"))
					.project("the_geom,tot_oa_cd,base_year,age_intvl,total")
					.storeByGroup(Group.ofKeys("base_year"), RESULT, GEOMETRY(gcInfo))
					.build();
		marmot.execute(plan);
//		marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
//		marmot.createDataSet(RESULT, plan, DataSetOption.GEOMETRY(gcInfo), DataSetOption.FORCE);
		watch.stop();
		
//		for ( DataSet result: marmot.getDataSetAllInDir(RESULT, true) ) {
//			SampleUtils.printPrefix(result, 5);
//		}
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
