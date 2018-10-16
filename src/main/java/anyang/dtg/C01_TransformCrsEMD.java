package anyang.dtg;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
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
		plan = marmot.planBuilder("전국_읍면동 좌표계 변환")
					.load(EMD)
					.transformCrs("the_geom", gcInfo.srid(), dtgInfo.srid())
					.project("the_geom,emd_cd,emd_kor_nm")
					.build();
		DataSet result = marmot.createDataSet(OUTPUT, plan, GEOMETRY(dtgInfo), FORCE);
		result.cluster(ClusterDataSetOptions.create().workerCount(1));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		marmot.disconnect();
	}
}
