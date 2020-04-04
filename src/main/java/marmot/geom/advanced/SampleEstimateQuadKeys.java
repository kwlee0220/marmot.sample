package marmot.geom.advanced;

import java.util.Set;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.geo.command.EstimateQuadKeysOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleEstimateQuadKeys {
	private static final String SIDO = "구역/시도";
//	private static final String INPUT = "교통/지하철/서울역사";
//	private static final String INPUT = "구역/집계구";
//	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "구역/연속지적도_2017";
	private static final String INPUT = "교통/dtg_2016";
//	private static final String INPUT = "나비콜/택시로그";
	
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
		
		long sampleSize = UnitUtils.parseByteSize("256mb");
		long clusterSize = UnitUtils.parseByteSize("1gb");
		EstimateQuadKeysOptions opts = EstimateQuadKeysOptions.DEFAULT()
//															.mapperCount(71)
															.validRange(bounds)
															.sampleSize(sampleSize)
															.maxQuadKeyLength(19)
															.clusterSize(clusterSize);
		Set<String> quadKeys = input.estimateQuadKeys(opts);
		FStream.from(quadKeys)
				.forEach(System.out::println);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
