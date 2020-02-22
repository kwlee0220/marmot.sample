package marmot.module;

import org.apache.log4j.PropertyConfigurator;

import marmot.analysis.module.geo.ClusterSpatialDataSetParameters;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleClusterSpatialDataSet {
	private static final String INPUT = "교통/dtg_201609";
//	private static final String INPUT = "주소/건물POI";
//	private static final String INPUT = "POI/병원";
	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet(INPUT);
		
		ClusterSpatialDataSetParameters params = new ClusterSpatialDataSetParameters();
		params.inputDataset(INPUT);
		params.outputDataset(OUTPUT);
		params.maxQuadKeyLength(17);
		params.clusterSize("6gb");
//		params.clusterSize("64mb");
//		params.sampleRatio((double)params.clusterSize() / input.length());
		params.sampleRatio(0.002);
		params.force(true);
		params.compressionCodecName("lz4");
//		params.workerCount(7);
		params.workerCount(53);
		marmot.executeProcess("cluster_spatial_dataset", params.toMap());
		
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
