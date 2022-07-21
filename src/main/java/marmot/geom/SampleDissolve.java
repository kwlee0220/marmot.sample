package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleDissolve {
	private static final String RESULT = "tmp/sample/result";
	private static final String INPUT = "교통/지하철/서울역사";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("dissolve")
								.load(INPUT)
								.project("the_geom,sig_cd")
								.dissolve("sig_cd", "the_geom")
								.store(RESULT, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
