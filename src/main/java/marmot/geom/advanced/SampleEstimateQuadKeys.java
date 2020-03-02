package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.optor.StoreDataSetOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleEstimateQuadKeys {
	private static final String SIDO = "구역/시도";
//	private static final String INPUT = "교통/dtg_201609";
	private static final String INPUT = "나비콜/택시로그";
//	private static final String INPUT = "교통/지하철/서울역사";
//	private static final String INPUT = "주소/건물POI";
	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		DataSet refDs = marmot.getDataSet(SIDO);
		Envelope bounds = refDs.getBounds();
		if ( !refDs.getGeometryColumnInfo().srid().equals(gcInfo.srid()) ) {
			bounds = CoordinateTransform.get(refDs.getGeometryColumnInfo().srid(), gcInfo.srid())
										.transform(bounds);
		}
		
		long clusterSize = UnitUtils.parseByteSize("64mb");
		
		Plan plan;
		plan = Plan.builder("test_estimate_quadkeys")
					.load(INPUT)
					.estimateQueryKeys(gcInfo, 0.001, FOption.of(bounds), 17, clusterSize)
					.store(OUTPUT, StoreDataSetOptions.FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		DataSet result = marmot.getDataSet(OUTPUT);
		SampleUtils.printPrefix(result, 5);
	}
}
