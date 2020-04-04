package demo.policy;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step01 {
	private static final String INPUT = "POI/노인복지시설";
	static final String TEMP = "tmp/result";
	static final String RESULT = "tmp/10min/step01";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = Plan.builder("노인복지시설_경로당_추출_버퍼400")
						.load(INPUT)
						.filter("induty_nm == '경로당'")		// (1) 영역분석
						.buffer(gcInfo.name(), 400)				// (2) 버퍼추정
						.store(TEMP, StoreDataSetOptions.FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		System.out.printf("elapsed time=%s (processing)%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(TEMP);
		result.clusterSpatially(RESULT, ClusterSpatiallyOptions.FORCE());
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
