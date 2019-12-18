package anyang.dtg;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class C01_TransformCrsEMD {
	private static final String EMD = "구역/읍면동";
	private static final String DTG = "교통/dtg";
	private static final String OUTPUT = "분석결과/안양대/네트워크/읍면동_wgs84";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet emd = marmot.getDataSet(EMD);
		GeometryColumnInfo gcInfo = emd.getGeometryColumnInfo();
		GeometryColumnInfo dtgInfo = marmot.getDataSet(DTG).getGeometryColumnInfo();

		Plan plan;
		plan = Plan.builder("전국_읍면동 좌표계 변환")
					.load(EMD)
					.transformCrs("the_geom", gcInfo.srid(), dtgInfo.srid())
					.project("the_geom,emd_cd,emd_kor_nm")
					.store(OUTPUT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(OUTPUT);
		result.cluster(ClusterDataSetOptions.WORKER_COUNT(1));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		marmot.close();
	}
}
