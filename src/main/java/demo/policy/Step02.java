package demo.policy;

import utils.StopWatch;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step02 {
	private static final String INPUT = "주민/인구밀도_2000";
	static final String RESULT = "tmp/10min/step02";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder("인구밀도_2017_중심점추출_10000이상")
						.load(INPUT)
						.filter("value >= 10000")		// (5) 영역분석
						.centroid(gcInfo.name())		// (4) 중심점 추출
						.store(RESULT, StoreDataSetOptions.FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		System.out.printf("elapsed time=%s (processing)%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(RESULT);
		result.cluster(ClusterSpatiallyOptions.FORCE);
		
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
}
