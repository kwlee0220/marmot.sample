package marmot.geom;

import static marmot.DataSetOption.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIntersectionJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "POI/주유소_가격";
	private static final String INNER = "tmp/서울특별시";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		SampleUtils.writeSeoul(marmot, INNER);
		
		Plan plan = marmot.planBuilder("sample_intersection_join")
							.load(OUTER)
							.buffer("the_geom", 50)
							.intersectionJoin("the_geom", INNER)
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE);
		marmot.deleteDataSet(INNER);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
