package marmot.geom;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadSquareGridFile {
//	private static final String INPUT = "교통/지하철/서울역사";
	private static final String INPUT = "구역/시군구";
	private static final String RESULT = "tmp/result";
	private static final double SIDE_LEN = 600;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		DataSet dataset = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = dataset.getGeometryColumnInfo();
		Size2d dim = new Size2d(SIDE_LEN, SIDE_LEN);

		Plan plan = Plan.builder("sample_load_squaregrid")
							.loadGrid(new SquareGrid(INPUT, dim))
							.spatialSemiJoin("the_geom", INPUT)
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
