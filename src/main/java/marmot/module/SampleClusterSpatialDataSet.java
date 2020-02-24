package marmot.module;

import org.apache.log4j.PropertyConfigurator;

import marmot.analysis.module.geo.ClusterSpatialDataSetParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleClusterSpatialDataSet {
	private static final String INPUT = "교통/dtg_201609";
//	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "POI/병원";
//	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		long clusterSize = UnitUtils.parseByteSize("4gb");
		long sampleSize = UnitUtils.parseByteSize("256mb");
		long blockSize = UnitUtils.parseByteSize("128mb");
		int workerCount = 53;
		
		ClusterSpatialDataSetParameters params = new ClusterSpatialDataSetParameters();
		params.inputDataSet(INPUT);
		params.outputDataSet(INPUT + "_clustered");
		params.maxQuadKeyLength(17);
		params.clusterSize(clusterSize);
		params.sampleSize(sampleSize);
		params.force(true);
		params.compressionCodecName("lz4");
		params.blockSize(blockSize);
		params.workerCount(workerCount);
		marmot.executeProcess("cluster_spatial_dataset", params.toMap());
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
