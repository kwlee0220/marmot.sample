package marmot.geom;

import static marmot.StoreDataSetOptions.*;
import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadHexagonGridFile {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	private static final double SIDE_LEN = 150;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet dataset = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = dataset.getGeometryColumnInfo();
		Envelope bounds = dataset.getBounds();
		bounds.expandBy(2*SIDE_LEN, SIDE_LEN);

		Plan plan = marmot.planBuilder("load_hexagon_grid")
							.loadHexagonGridFile(bounds, gcInfo.srid(), SIDE_LEN, 8)
							.spatialSemiJoin("the_geom", INPUT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
