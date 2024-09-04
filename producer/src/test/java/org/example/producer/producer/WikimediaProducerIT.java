package org.example.producer.producer;

import com.google.gson.Gson;
import com.wikimedia.Meta;
import com.wikimedia.RecentChange;
import org.example.producer.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.mockito.Mockito.*;

@SpringBootTest
@DirtiesContext
@Import(Config.class)
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class WikimediaProducerIT {
    @MockBean
    private Gson gson;

    @Test
    public void loadServerSentEventAndSentToKafkaTest() {
        Meta meta = new Meta("https://example.com", "req123", "id456", "2024-09-04T17:29:13Z", "example.com", "stream1", "topic1", 1, 100L);
        RecentChange recentChange = new RecentChange(meta, 123456L, "edit", 0, "Dummy Title", "https://example.com/Dummy_Title", "This is a dummy comment.", 1693840153000L, // Unix timestamp for 2024-09-04T17:29:13Z
                "DummyUser", false, "https://example.com", "example.com", "/w", "dummywiki");
        when(gson.fromJson(eq("data"), eq(RecentChange.class))).thenReturn(recentChange);

        verify(gson).fromJson(eq("data"), eq(RecentChange.class));
    }
}
