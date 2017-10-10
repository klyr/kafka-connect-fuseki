package kafka.connect.fuseki;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.RetriableException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.HttpUrl;;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class FusekiSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(FusekiSinkTask.class);
    public static final MediaType JSONLD = MediaType.parse("application/ld+json");

    private ObjectMapper jsonMapper;
    private OkHttpClient fusekiClient;
    private HttpUrl fusekiUrl;

    private String fusekiServer;
    private String fusekiDataset;

    public FusekiSinkTask() {
	jsonMapper = new ObjectMapper();
    }

    @Override
    public String version() {
	return new FusekiSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
	fusekiServer = props.get(FusekiSinkConnector.FUSEKI_SERVER);
	fusekiDataset = props.get(FusekiSinkConnector.FUSEKI_DATASET);

	fusekiClient = new OkHttpClient.Builder()
	    .connectTimeout(5, TimeUnit.SECONDS)
	    .writeTimeout(3000, TimeUnit.MILLISECONDS)
	    .readTimeout(3000, TimeUnit.MILLISECONDS)
	    .build();

	fusekiUrl = HttpUrl.parse(fusekiServer).newBuilder()
	    .addPathSegment(fusekiDataset)
	    .build();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
	if (sinkRecords.isEmpty()) {
	    log.debug("No Records to process");
	    return;
	}

	SinkRecord first = sinkRecords.iterator().next();
	log.debug("Received {} records. Topic-Partition-Offset: {}-{}-{}", sinkRecords.size(),
		  first.topic(), first.kafkaPartition(), first.kafkaOffset());

	for (SinkRecord record: sinkRecords) {
	    try {
		JsonNode parsedJson = jsonMapper.readTree((byte[]) record.value());
	    } catch (JsonProcessingException e) {
		log.warn("Dropping invalid JSON (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    } catch (IOException e) {
		log.warn("Dropping because of unhandled exception (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    }

	    RequestBody body = RequestBody.create(JSONLD, (byte[]) record.value());
	    Request request = new Request.Builder()
		.url(fusekiUrl)
		.header("Content-Type", "application/ld+json")
		.post(body)
		.build();

	    Response response = null;
	    try {
		response = fusekiClient.newCall(request).execute();
		if (!response.isSuccessful()) {
		    log.warn("This document was not accepted by Fuseki, maybe it's an invalid JSON-LD document ({})",
			     response.message());
		}
	    } catch (IOException e) {
		log.error("Error while sending data to Fuseki: {}", e.toString());
		throw new RetriableException("Connection to Fuseki server has been lost");
	    } finally {
		if (response != null && response.body() != null) {
		    response.body().close();
		}
	    }
	}
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
	log.debug("flush() called");
    }

    @Override
    public void stop() {
	log.debug("stop() called");
    }
}

