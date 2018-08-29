package basic;

import org.apache.log4j.PropertyConfigurator;

import com.google.protobuf.util.JsonFormat;

import marmot.Plan;
import marmot.RecordSchema;
import marmot.command.MarmotCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

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

		Plan plan;
		plan = marmot.planBuilder("import_plan")
					.expand("휘발유:int,경유:int")
					.toPoint("xpos", "ypos", "the_geom")
					.transformCrs("the_geom", "EPSG:5179", "the_geom", "EPSG:5186")
					.project("the_geom,*-{the_geom,xpos,ypos}")
//					.assignUid("id")
//					.parseCsv(schema, ',', '\\', Option.none(), true)
//					.toPoint("xpos", "ypos", "the_geom")
//					.update("if (경도 < 88 && 위도 > 88) {_xx = 경도; 경도 = 위도; 위도 = _xx; }")
//					.update("기준년도=(기준년도.length() > 0) ? 기준년도 : '2017'; 기준월=(기준월.length() > 0) ? 기준월 : '01'")
//					.expand("기준년도:short,기준월:short", "기준년도=기준년도; 기준월=기준월;")
					.build();
		
		System.out.println(JsonFormat.printer().print(plan.toProto()));
	}
}
