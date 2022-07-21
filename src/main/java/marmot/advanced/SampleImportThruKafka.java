package marmot.advanced;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.command.MarmotConnector;
import marmot.dataset.DataSet;
import marmot.protobuf.PBRecordProtos;
import marmot.remote.protobuf.PBMarmotClient;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleImportThruKafka {
	private static final String TINY = "교통/지하철/서울역사";
	private static final String SMALL = "나비콜/택시로그";
	private static final String MEDIUM = "건물/건물통합정보마스터";
	private static final String LARGE = "구역/연속지적도_2019";
	private static final String RANGE = "구역/행정동코드";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MarmotConnector.getKafkaBroker().get());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
	
		importSubway(marmot, props);
	}

	private static final void importSubway(MarmotRuntime marmot, Properties props) {
		KafkaProducer<String,byte[]> producer = new KafkaProducer<>(props);
		DataSet result = marmot.getDataSet(TINY);
		try ( RecordSet rset = result.read() ) {
			Record rec = DefaultRecord.of(rset.getRecordSchema());
			while ( rset.next(rec) ) {
				byte[] bytes = PBRecordProtos.toProto(rec).toByteArray();
				ProducerRecord<String,byte[]> prec = new ProducerRecord<>("marmot_kafka_import", "tmp/subway", bytes);
				producer.send(prec);
			}
		}
		finally {
			producer.close();
		}
	}
}
