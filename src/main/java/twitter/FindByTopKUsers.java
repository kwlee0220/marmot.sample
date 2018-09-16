package twitter;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotCommands;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByTopKUsers {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";

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
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		List<String> userIds = findTopKUsers(marmot);
		System.out.println("topk_users=" + userIds);
				
		DataSet info = marmot.getDataSet(TWEETS);
			
		String userIdsStr = userIds.stream().collect(Collectors.joining(","));
		String inializer = String.format("$target_users = Lists.newArrayList(%s)", userIdsStr);
		String pred = "$target_users.contains(user_id)";
		Plan plan = marmot.planBuilder("find_by_userids")
								.load(TWEETS)
								.filter(inializer, pred)
								.project("the_geom,id")
								.store(RESULT)
								.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
															info.getGeometryColumnInfo().srid());
		DataSet result = marmot.createDataSet(RESULT, gcInfo, plan, DataSetOption.FORCE);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}

	public static List<String> findTopKUsers(MarmotRuntime marmot) throws Exception {
		// 가장 자주 tweet을 한 사용자 식별자들을 저장할 임시 파일 이름을 생성한다.
		String tempFile = "tmp/" + UUID.randomUUID().toString();

		Plan plan = marmot.planBuilder("list_topk_users")
								.load(TWEETS)
								.groupBy("user_id").aggregate(AggregateFunction.COUNT())
								.pickTopK("count:D", 5)
								.storeMarmotFile(tempFile)
								.build();
		marmot.execute(plan);
		SampleUtils.printMarmotFilePrefix(marmot, tempFile, 10);
		
		try {
			return marmot.readMarmotFile(tempFile)
						.stream()
						.map(rec -> rec.getString("user_id"))
						.collect(Collectors.toList());
		}
		finally {
			marmot.deleteFile(tempFile);
		}
	}
}
