package demo.poc.age;

import java.util.ArrayList;
import java.util.List;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.exec.CompositeAnalysis;
import marmot.exec.MarmotAnalysis;
import marmot.exec.PlanAnalysis;
import marmot.optor.AggregateFunction;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AddAgeInterval1 {
	private static final String ANALYSIS = "도시공간구조1";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		marmot.deleteAnalysis(ANALYSIS, true);
	
		List<MarmotAnalysis> components = new ArrayList<>();
		
		collectPopulation(marmot, 2000, components);
		collectPopulation(marmot, 2005, components);
		collectPopulation(marmot, 2010, components);
		collectPopulation(marmot, 2015, components);
		
		List<String> compIds = new ArrayList<>();
		for ( MarmotAnalysis anal: components ) {
			marmot.addAnalysis(anal, true);
			compIds.add(anal.getId());
		}
		marmot.addAnalysis(new CompositeAnalysis(ANALYSIS, compIds), true);
	}
	
	private static void collectPopulation(MarmotRuntime marmot, int year,
											List<MarmotAnalysis> components) {
		String id = String.format("%d년도_연령대별_인구_수집", year);
		String inDsId = String.format("주민/성연령별인구/%d년", year);
		String outDsId = String.format("pop_age_interval_%d", year);
		
		DataSet stations = marmot.getDataSet(inDsId);
		GeometryColumnInfo gcInfo = stations.getGeometryColumnInfo();
		
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
		components.add(new PlanAnalysis(ANALYSIS + "/" + id, plan));
	}
}
