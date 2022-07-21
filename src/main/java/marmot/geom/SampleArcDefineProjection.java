package marmot.geom;

import utils.StopWatch;
import utils.func.FOption;

import common.SampleUtils;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleArcDefineProjection {
	private static final String INPUT = "안양대/공간연산/define_projection/input";
	private static final String RESULT = "tmp/result";

	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo(ds.getGeometryColumn(), "EPSG:5179");
		DataSet result = ds.updateGeometryColumnInfo(FOption.of(gcInfo));
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
	}
}
