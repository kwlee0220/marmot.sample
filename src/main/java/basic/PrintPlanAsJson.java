package basic;

import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.util.JsonFormat;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CSV;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PrintPlanAsJson {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		String outSchemaExpr = "the_geom:point,id:string,user_id:string,created_at:string,"
							+ "coordinates:point,text:string";
		RecordSchema schema = RecordSchema.parse(outSchemaExpr);
		String initExpr = "$format=ST_DTPattern('EEE MMM dd HH:mm:ss Z yyyy').withLocale(Locale.ENGLISH)";
		String transExpr = "local:_meta.mvel";
		
		String[] header = FStream.range(0, 13)
								.map(idx -> String.format("field_%02d", idx))
								.toArray(String.class);
		header = CSV.get()
//					.parse("date,owner,car_no,time,mileage,mileage_accum,velo,rpm,brake,xpos,ypos,heading,xacc,yacc")
//					.parse("번호,사업자명,소재지전체주소,도로명주소,인허가일자,형태,경도,위도")
					.parse("시군구코드,출입구일련번호,법정동코드,시도명,시군구명,읍면동명,도로명코드,도로명,지하여부,건물본번,건물부번,건물명,우편번호,건물용도분류,건물군여부,관할행정동,xpos,ypos")
					.toArray(new String[0]);
		

		Plan plan;
		plan = marmot.planBuilder("import_plan")
//					.expand("카메라대수:int,카메라화소수:int,보관일수:int")
//					.project("*-{데이터기준일자}")
//					.assignUid("id")
					.parseCsv(header, ',', '"', "")
					.toPoint("xpos", "ypos", "the_geom")
					.transformCrs("the_geom", "EPSG:4326", "the_geom", "EPSG:5186")
					.expand("지하여부:byte,건물군여부:byte")
					.project("the_geom,*-{xpos,ypos,the_geom}")
//					.update("if (경도 < 88 && 위도 > 88) {_xx = 경도; 경도 = 위도; 위도 = _xx; }")
//					.update("기준년도=(기준년도.length() > 0) ? 기준년도 : '2017'; 기준월=(기준월.length() > 0) ? 기준월 : '01'")
//					.expand("기준년도:short,기준월:short", "기준년도=기준년도; 기준월=기준월;")
					.build();
		
		System.out.println(JsonFormat.printer().print(plan.toProto()));
	}
}
