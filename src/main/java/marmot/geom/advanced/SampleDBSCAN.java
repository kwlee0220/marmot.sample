package marmot.geom.advanced;

import static marmot.optor.AggregateFunction.CONVEX_HULL;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.analysis.module.geo.DBSCANParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.StoreDataSetOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleDBSCAN {
	private static final String HOSTPITAL = "POI/병원";
	private static final String INPUT = "tmp/hospital";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		DataSet ds = marmot.getDataSet(HOSTPITAL);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder()
					.load(HOSTPITAL)
					.assignUid("id")
					.defineColumn("id:string", "String.format('%010d', id)")
					.store(INPUT, StoreDataSetOptions.FORCE(ds.getGeometryColumnInfo()))
					.build();
		marmot.execute(plan);
		
		DBSCANParameters params = new DBSCANParameters();
		params.inputDataSet(INPUT);
		params.outputDataSet(RESULT);
		params.radius(500);
		params.minCount(5);
		params.idColumn("id");
		params.clusterColumn("cluster_id");
		params.typeColumn("cluster_type");
//		marmot.executeProcess("dbscan", params.toMap());
		
		plan = Plan.builder()
					.load(RESULT)
					.aggregateByGroup(Group.ofKeys("cluster_id"), CONVEX_HULL(gcInfo.name()))
					.store("tmp/result2", StoreDataSetOptions.FORCE(gcInfo))
					.build();
		marmot.execute(plan);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		DataSet input = marmot.getDataSet(INPUT);
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("src=%d, dest=%d%n", input.getRecordCount(), result.getRecordCount());
		SampleUtils.printPrefix(result, 5);
	}
}
