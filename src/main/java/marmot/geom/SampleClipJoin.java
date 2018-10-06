package marmot.geom;

import static marmot.DataSetOption.FORCE;
import static marmot.DataSetOption.GEOMETRY;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleClipJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "POI/주유소_가격";
	private static final String INNER = "시연/서울특별시";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClient.connect();
		
		GeometryColumnInfo gcInfo = marmot.getDataSet(OUTER).getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("sample_clip_join")
								.load(OUTER)
								.clipJoin("the_geom", INNER)
								.build();
		DataSet result = marmot.createDataSet(RESULT, plan, GEOMETRY(gcInfo), FORCE);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
