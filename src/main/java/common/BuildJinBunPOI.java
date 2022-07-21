package common;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.UUID;

import utils.StopWatch;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildJinBunPOI {
	private static final String JIBUN = "주소/지번";
	private static final String ADDR = "주소/주소";
	private static final String BUILD_POI = "주소/건물POI";
	private static final String RESULT = "주소/지번POI";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan;
		DataSet result;
		RecordSchema schema;

		String tempDs = "tmp/" + UUID.randomUUID().toString();
		plan = Plan.builder("distinct_jibun")
					.load(JIBUN)
					.distinct("건물관리번호", 11) 
					.store(tempDs, FORCE)
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(tempDs);

		try {
			DataSet ds = marmot.getDataSet(BUILD_POI);
			String geomCol = ds.getGeometryColumn();
			GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
			
			plan = Plan.builder("build_jibun_poi")
							.load(BUILD_POI)
							.project(geomCol + ",도로명코드,건물본번,건물부번,지하여부,법정동코드")
							.hashJoin("도로명코드,건물본번,건물부번,지하여부",
									ADDR, "도로명코드,건물본번,건물부번,지하여부",
									geomCol + ",param.{건물관리번호}",
									JoinOptions.INNER_JOIN(23))
							.hashJoin("건물관리번호", tempDs, "건물관리번호",
									"*,param.{법정동코드,지번본번,지번부번,산여부}",
									JoinOptions.INNER_JOIN)
							.distinct("건물관리번호,법정동코드,지번본번,지번부번,산여부")
							.store(RESULT, FORCE(gcInfo))
							.build();
			marmot.execute(plan);
			
			result = marmot.getDataSet(RESULT);
			System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
			
			SampleUtils.printPrefix(result, 5);
		}
		finally {
			marmot.deleteDataSet(tempDs);
		}
	}
}
