package marmot.geom.advanced;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleE2SFCASetup {
	private static final String FLOW_POP = "주민/유동인구/월별_시간대/2015";
	private static final String SGG = "구역/시군구";
	private static final String RESULT = "주민/유동인구/강남구/시간대/2015";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		Geometry gangnaum = getGangnamGu(marmot);
		
		GeometryColumnInfo GC_INFO = new GeometryColumnInfo("the_geom", "EPSG:5186");
		plan = Plan.builder("강남구 영역 유동인구 정보 추출")
						.query(FLOW_POP, gangnaum)
						.expand("year:int", "year = std_ym.substring(0,4)")
						.aggregateByGroup(Group.ofKeys("block_cd,year").tags("the_geom"),
											AVG("avg_08tmst").as("avg_08tmst"),
											AVG("avg_15tmst").as("avg_15tmst"))
						.project("*-{year}")
						.transformCrs("the_geom", "EPSG:5179", "EPSG:5186")
						.store(RESULT, FORCE(GC_INFO))
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static Geometry getGangnamGu(MarmotRuntime marmot) {
		Plan plan;
		plan = Plan.builder("강남구 추출")
					.load(SGG)
					.filter("sig_cd.startsWith('11') && sig_kor_nm == '강남구'")
					.project("the_geom")
					.build();
		return marmot.executeLocally(plan).toList().get(0).getGeometry("the_geom");
	}
}
