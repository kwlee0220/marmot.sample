package bizarea;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
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
public class Step1Building {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String BUILDINGS = "건물/통합정보";
	private static final String RESULT = "tmp/bizarea/grid100_land";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		avgExpr = String.format("flow_pop=(%s)/24", avgExpr);
		
		DataSet ds = marmot.getDataSet(BIZ_GRID);
		String geomCol = ds.getGeometryColumn();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = marmot.planBuilder("대도시 상업지역 구역별 건축물 수와 면적 집계")
							.load(BUILDINGS)
							// BIZ_GRID와 소지역 코드를 이용하여 조인하여,
							// 대도시 상업지역과 겹치는 건축물 구역을 뽑는다. 
							.spatialJoin("the_geom", BIZ_GRID, "건축물용도코드,대지면적,param.*")
							// 그리드 셀, 건축물 용도별로 건물 수와 총 면점을 집계한다. 
							.aggregateByGroup(Group.ofKeys("cell_id,block_cd,건축물용도코드")
													.tags(geomCol + ",sgg_cd")
													.workerCount(3),
												SUM("대지면적").as("대지면적"),
												COUNT().as("bld_cnt"))
							.project(String.format("%s,*-{%s}", geomCol, geomCol))
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
