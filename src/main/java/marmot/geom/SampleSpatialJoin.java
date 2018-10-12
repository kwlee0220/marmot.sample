package marmot.geom;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialJoin {
	private static final String RESULT = "tmp/result";
	private static final String GAS_STATIONS = "POI/주유소_가격";
	private static final String EMD = "구역/읍면동";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Plan plan = marmot.planBuilder("spatial_join")
							.load(GAS_STATIONS)
							.spatialJoin("the_geom", EMD, "*,param.emd_kor_nm as emd_name")
							.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
