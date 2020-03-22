package marmot.module;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.analysis.module.geo.ClusterSpatialDataSetParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleClusterSpatialDataSet {
	private static final String BOUNDS = "구역/시도";
//	private static final String QUAD_INPUT = "지오비전/집계구/2015_clustered";
//	private static final String BOUNDS = "교통/지하철/서울역사";
//	private static final String INPUT = "교통/dtg_201609";
	private static final String INPUT = "나비콜/택시로그";
//	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "POI/병원";
//	private static final String INPUT = "지오비전/집계구/2015";
//	private static final String INPUT = "구역/시군구";
//	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		long clusterSize = UnitUtils.parseByteSize("64mb");
		long sampleSize = UnitUtils.parseByteSize("64mb");
		long blockSize = UnitUtils.parseByteSize("64mb");
//		long clusterSize = UnitUtils.parseByteSize("4gb");
//		long sampleSize = UnitUtils.parseByteSize("256mb");
//		long blockSize = UnitUtils.parseByteSize("128mb");
		int workerCount = 53;
		
		DataSet refDs = marmot.getDataSet(BOUNDS);
		Envelope bounds = refDs.getBounds();
		if ( !refDs.getGeometryColumnInfo().srid().equals(gcInfo.srid()) ) {
			bounds = CoordinateTransform.get(refDs.getGeometryColumnInfo().srid(), gcInfo.srid())
										.transform(bounds);
		}
		
		ClusterSpatialDataSetParameters params = new ClusterSpatialDataSetParameters();
		params.inputDataSet(INPUT);
		params.outputDataSet(INPUT + "_clustered");
		params.validBounds(bounds);
		params.maxQuadKeyLength(17);
		params.clusterSize(clusterSize);
		params.sampleSize(sampleSize);
//		params.quadKeyDataSet(QUAD_INPUT);
		params.force(true);
		params.compressionCodecName("lz4");
		params.blockSize(blockSize);
		params.partitionCount(workerCount);
		marmot.executeProcess(ClusterSpatialDataSetParameters.moduleName(), params.toMap());
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
