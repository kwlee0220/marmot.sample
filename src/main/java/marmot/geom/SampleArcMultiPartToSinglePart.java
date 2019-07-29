package marmot.geom;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.UNION_GEOM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Column;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcMultiPartToSinglePart {
	private static final String INPUT = "안양대/공간연산/multipart_to_singlepart/input";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		String geomCol = input.getGeometryColumn();
		String tags = input.getRecordSchema().streamColumns()
							.map(Column::name)
							.filter(n -> !n.equals("name"))
							.filter(n -> !n.equals(geomCol))
							.join(',');
		
		Plan plan = marmot.planBuilder("spatial_join")
							.load(INPUT)
							.aggregateByGroup(Group.ofKeys("name").tags(tags),
												UNION_GEOM("the_geom"))
							.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
