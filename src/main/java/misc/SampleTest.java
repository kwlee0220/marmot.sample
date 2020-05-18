package misc;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.plan.Group;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleTest {
	private static final String PARAM = "구역/행정동코드";
	private static final String TEST_PARAM = "tmp/param";
	private static final String INPUT = "교통/dtg_201609";
//	private static final String INPUT = "나비콜/택시로그";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet param = marmot.getDataSet(PARAM);
		GeometryColumnInfo fromGcInfo = param.getGeometryColumnInfo();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo toGcInfo = input.getGeometryColumnInfo();
		
		Plan plan;
		plan = Plan.builder()
					.load(PARAM)
					.project("the_geom,hcode")
					.transformCrs(param.getGeometryColumn(), fromGcInfo.srid(), toGcInfo.srid())
					.store(TEST_PARAM, FORCE(toGcInfo))
					.build();
		marmot.execute(plan);
		param = marmot.getDataSet(TEST_PARAM);
		param.cluster(ClusterSpatiallyOptions.FORCE);

		plan = Plan.builder("group_by")
//					.load(INPUT, LoadOptions.FIXED_MAPPERS)
					.load(INPUT)
					.spatialJoin("the_geom", TEST_PARAM, "right.hcode")
					.aggregateByGroup(Group.ofKeys("hcode"), COUNT())
					.store(RESULT, FORCE)
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		SampleUtils.printPrefix(result, 5);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedSecondString());
	}
}
