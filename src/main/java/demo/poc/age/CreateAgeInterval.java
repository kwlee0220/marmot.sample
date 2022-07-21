package demo.poc.age;

import utils.StopWatch;

import marmot.BindDataSetOptions;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateAgeInterval {
	public static final void main(String... args) throws Exception {
		System.out.println("시작: 도시공간구조 분석: 데이터 준비...... ");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		int[] YEARS = new int[] {2000, 2005, 2010, 2015};
		for ( int year: YEARS ) {
			String inDsId = String.format("주민/성연령별인구/%d년", year);
			String outDsId = String.format("pop_age_interval_%d", year);
			
			createAgePopulation(marmot, year, inDsId, outDsId);
		}
	}
	
	private static void createAgePopulation(MarmotRuntime marmot, int year, String inDsId,
											String outDsId) {
		String id = String.format("%d년도_연령대별_인구_수집", year);
		System.out.printf("단계: %s -> ", id); System.out.flush();
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(inDsId);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder(id)
					.load(inDsId)
					.defineColumn("base_year:int")
					.filter("base_year == " + year)
					.defineColumn("age_intvl:int", "(item_name.substring(7) / 10) * 10")
					.aggregateByGroup(Group.ofKeys("tot_oa_cd,age_intvl").tags("the_geom"),
									AggregateFunction.SUM("value").as("total"))
					.project("the_geom,tot_oa_cd,age_intvl,total")
					.flattenGeometry(gcInfo.name(), DataType.POLYGON)
					.shard(1)
					.storeAsGhdfs(outDsId, gcInfo, true)
					.build();
		marmot.execute(plan);
		marmot.bindExternalDataSet(outDsId, outDsId, DataSetType.GWAVE, BindDataSetOptions.FORCE(true));

		ds = marmot.getDataSet(outDsId);
		System.out.printf("%s(%d건), 소요시간=%ss%n",
							outDsId, ds.getRecordCount(), watch.getElapsedMillisString());
	}
}
