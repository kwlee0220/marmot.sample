package basic;

import static marmot.plan.ParseCsvOption.HEADER;
import static marmot.plan.ParseCsvOption.NULL_STRING;
import static marmot.plan.ParseCsvOption.*;

import java.text.DecimalFormat;

import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.util.JsonFormat;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.plan.RecordScript;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.type.DataType;
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
//					.parse("car_no,ts,month,sid_cd,besselX,besselY,status,company,driver_id,xpos,ypos")
//					.parse("번호,사업자명,소재지전체주소,도로명주소,인허가일자,형태,경도,위도")
//					.parse("시군구코드,출입구일련번호,법정동코드,시도명,시군구명,읍면동명,도로명코드,도로명,지하여부,건물본번,건물부번,건물명,우편번호,건물용도분류,건물군여부,관할행정동,xpos,ypos")
//					.parse("시군구,번지,본번,부번,단지명,전용면적,계약년월,계약일,거래금액,층,건축년도,도로명")
					.parse("사용년월,대지위치,도로명_대지위치,시군구코드,법정동코드,대지구분코드,번,지,새주소_일련번호,새주소_도로코드,새주소_지상지하코드,새주소_본번,새주소_부번,사용량")
//					.parse("시군구,번지,본번,부번,단지명,전월세구분,전용면적,계약년월,계약일,보증금,월세,층,건축년도,도로명")
//					.parse("STD_YM,BLOCK_CD,X_COORD,Y_COORD,AVG_00TMST,AVG_01TMST,AVG_02TMST,AVG_03TMST,AVG_04TMST,AVG_05TMST,AVG_06TMST,AVG_07TMST,AVG_08TMST,AVG_09TMST,AVG_10TMST,AVG_11TMST,AVG_12TMST,AVG_13TMST,AVG_14TMST,AVG_15TMST,AVG_16TMST,AVG_17TMST,AVG_18TMST,AVG_19TMST,AVG_20TMST,AVG_21TMST,AVG_22TMST,AVG_23TMST")
//					.parse("도로명코드,도로명,도로명로마자,읍면동_일련번호,시도명,시도명로마자,시군구명,시군구명로마자,읍면동명,읍면동명로마자,읍면동구분,읍면동코드,사용여부,변경사유,변경이력,고시일자,말소일자")
					.toArray(new String[0]);
		
		String colDecls = FStream.range(0, 24)
							.map(idx -> String.format("AVG_%02dTMST:float", idx))
							.join(",");
		
		RecordScript script = RecordScript.of("$money_formatter = new DecimalFormat('#,###,###')",
												"$money_formatter.parse(거래금액).intValue();")
											.importClass(DecimalFormat.class);


		Plan plan;
		plan = marmot.planBuilder("import_plan")
//					.expand1("기준년도:short", "(기준년도.length() > 0) ? 기준년도 : '2017'")
//					.expand1("기준월:short", "(기준월.length() > 0) ? 기준월 : '01'")
//					.expand1("개별공시지가:long")
//					.project("고유번호,기준년도,기준월,개별공시지가")
					.expand("기준년도:short,개별공시지가:long")
					.project("고유번호 as pnu,기준년도,기준월,개별공시지가")
//					.assignUid("id")
//					.parseCsv(',', HEADER(header))
//					.parseCsv(',', HEADER(header), COMMENT('#'))
//					.parseCsv('|', HEADER(header))
//					.expand1("대지구분코드:int", "(int)대지구분코드 + 1")
//					.expand1("pnu:string", "시군구코드 + 법정동코드 + 대지구분코드 + 번 + 지")
//					.parseCsv('|', HEADER(header), TRIM_FIELD)
//					.expand("읍면동구분:byte,사용여부:byte,변경사유:byte")
//					.toPoint("xpos", "ypos", "the_geom")
//					.transformCrs("the_geom", "EPSG:5179", "EPSG:5186")
//					.transformCrs("the_geom", "EPSG:5179", "aaa", "EPSG:5186")
//					.update("기준년도=(기준년도.length() > 0) ? 기준년도 : '2017'; 기준월=(기준월.length() > 0) ? 기준월 : '01'")
//					.expand1("거래금액:int",script)
					.build();
		
		System.out.println(JsonFormat.printer().print(plan.toProto()));
	}
}
