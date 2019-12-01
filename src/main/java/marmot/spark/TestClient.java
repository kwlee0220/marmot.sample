package marmot.spark;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.remote.protobuf.PBMarmotSparkSessionClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestClient {
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		PBMarmotSparkSessionClient session = PBMarmotSparkSessionClient.connect("192.168.1.112", 5685);
		
		StopWatch watch = StopWatch.start();
		
		DataSet input = marmot.getDataSet("siggeo");
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		session.createOrReplaceView("sdgeo", "sdgeo");
		session.createOrReplaceView("siggeo", "siggeo");
		session.createOrReplaceView("emdgeo", "emdgeo");
		session.createOrReplaceView("blockgeo", "blockgeo");
		session.createOrReplaceView("flow_pop_time", "flow_pop_time");
		session.createOrReplaceView("dtg", "교통/dtg_201809");

/*
		String sql = "select count(*) from dtg";
		String sql = "select flow_pop_time "
					+ "from flow_pop_time "
					+ "where avg_10tmst > 30 "
					;
		
		String sql = "select flow_pop_time.avg_10tmst as c0 "
					+ "from flow_pop_time as flow_pop_time "
					+ "group by flow_pop_time.avg_10tmst "
					+ "order by flow_pop_time.avg_10tmst ASC";
		
		String sql2 = "select sdgeo.name as c0, siggeo.name as c1, "
						+ "avg(flow_pop_time.avg_10tmst) as m0 "
					+ "from sdgeo, siggeo, emdgeo, blockgeo, flow_pop_time "
					+ "where flow_pop_time.BLOCK_CD = blockgeo.BLOCK_CD "
					+ "and blockgeo.emdcode = emdgeo.emdcode "
					+ "and emdgeo.sigcode = siggeo.sigcode "
					+ "and siggeo.sdcode = sdgeo.sdcode "
					+ "and sdgeo.name = '서울특별시' "
					+ "and siggeo.name in ('강남구', '강동구', '강북구', '강서구', '관악구', '광진구', "
										+ "'구로구', '금천구', '노원구', '도봉구', '동대문구', "
										+ "'동작구', '마포구', '서대문구', '서초구', '성동구', "
										+ "'성북구', '송파구', '양천구', '영등포구', '용산구', "
										+ "'은평구', '종로구', '중구', '중랑구') "
					+ "group by sdgeo.name, siggeo.name, flow_pop_time.year";
*/

		String sql = "select sdgeo.name as c0, avg(flow_pop_time.avg_10tmst) as m0 "
					+ "from sdgeo, siggeo, emdgeo, blockgeo, flow_pop_time "
					+ "where flow_pop_time.BLOCK_CD = blockgeo.BLOCK_CD "
					+ "and blockgeo.emdcode = emdgeo.emdcode "
					+ "and emdgeo.sigcode = siggeo.sigcode "
					+ "and siggeo.sdcode = sdgeo.sdcode "
					+ "group by sdgeo.name";
/*
		String sql = "select sdgeo.name as c0, flow_pop_time.year as c1, "
						+ "avg(flow_pop_time.avg_10tmst) as m0 "
					+ "from sdgeo, siggeo, emdgeo, blockgeo, flow_pop_time "
					+ "where flow_pop_time.BLOCK_CD = blockgeo.BLOCK_CD "
					+ "and blockgeo.emdcode = emdgeo.emdcode "
					+ "and emdgeo.sigcode = siggeo.sigcode "
					+ "and siggeo.sdcode = sdgeo.sdcode "
					+ "and flow_pop_time.year = 2015 "
					+ "group by sdgeo.name, flow_pop_time.year";

//		String sql = "select sdgeo.name as c0, flow_pop_time.year as c1, "
//						+ "avg(flow_pop_time.avg_10tmst) as m0 "
//					+ "from sdgeo as sdgeo, siggeo as siggeo, emdgeo as emdgeo, "
//						+ "blockgeo as blockgeo, flow_pop_time as flow_pop_time "
//					+ "where flow_pop_time.BLOCK_CD = blockgeo.BLOCK_CD "
//					+ "and blockgeo.emdcode = emdgeo.emdcode "
//					+ "and emdgeo.sigcode = siggeo.sigcode "
//					+ "and siggeo.sdcode = sdgeo.sdcode "
//					+ "and flow_pop_time.year = 2015 "
//					+ "group by sdgeo.name, flow_pop_time.year";

//		String sql = "select sd.name, avg(avg_03tmst) as avg "
//					+ "from flow_pop_time, blockgeo, emd, sig, sd "
//					+ "where avg_01tmst > 10 "
//					+ "and flow_pop_time.BLOCK_CD = blockgeo.BLOCK_CD "
//					+ "and blockgeo.emdcode = emd.emdcode "
//					+ "and emd.sigcode = sig.sigcode "
//					+ "and sig.sdcode = sd.sdcode "
//					+ "group by sd.sdcode, sd.name ";
//		String sql = "select blockgeo.BLOCK_CD "
//					+ "from sd, sig, emd, blockgeo "
//					+ "where emd.sigcode = sig.sigcode "
//					+ "and sig.sdcode = sd.sdcode "
//					+ "and blockgeo.emdcode = emd.emdcode "
//					+ "and sig.name like '수원시%'";
//		String sql = "select count(*) from flow_pop_time";
 */
		
		session.runSql(sql, RESULT, FORCE);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		
		for ( int i =0; i < 5; ++i ) {
			StopWatch watch2 = StopWatch.start();
			session.runSql(sql, RESULT, FORCE);
			System.out.printf("elapsed=%s%n", watch2.stopAndGetElpasedTimeString());
		}
	}
}
