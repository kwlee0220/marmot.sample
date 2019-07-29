package marmot.geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.command.MarmotClientCommands;
import marmot.process.geo.arc.ArcBufferParameters;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcBuffer {
	private static final String INPUT = "안양대/공간연산/buffer/input";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		ArcBufferParameters params = new ArcBufferParameters();
		params.setInputDataset(INPUT);
		params.setOutputDataset(RESULT);
		params.setDistance(300);
		params.setDissolve(false);
		params.setForce(true);
		marmot.executeProcess("arc_buffer", params.toMap());

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		DataSet input = marmot.getDataSet(INPUT);
		DataSet result = marmot.getDataSet(RESULT);
		System.out.printf("src=%d, dest=%d%n", input.getRecordCount(), result.getRecordCount());
		SampleUtils.printPrefix(result, 5);
		

		params.setDissolve(true);
		marmot.executeProcess("arc_buffer", params.toMap());
		result = marmot.getDataSet(RESULT);
		System.out.printf("src=%d, dest=%d%n", input.getRecordCount(), result.getRecordCount());
		SampleUtils.printPrefix(result, 5);
	}
}
