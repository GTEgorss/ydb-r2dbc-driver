package tech.ydb.io.r2dbc.statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.io.r2dbc.helper.GrpcTransportRule;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.subscribers.LoggingQueryResultSubscriber;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;
import tech.ydb.query.tools.SessionRetryContext;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class YdbDDLStatementTest {
    @ClassRule
    public final static GrpcTransportRule ydbRule = new GrpcTransportRule();

    private static QueryClient client;
    private static SessionRetryContext retryCtx;

    @BeforeClass
    public static void init() {
        client = QueryClient.newClient(ydbRule)
                .sessionPoolMaxSize(5)
                .build();
        retryCtx = SessionRetryContext.create(client).build();

        assertNotNull(client.getScheduler());
    }

    @AfterClass
    public static void clean() {
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE episodes;", TxMode.NONE).execute()).join();
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE seasons;", TxMode.NONE).execute()).join();
        retryCtx.supplyResult(session -> session.createQuery("DROP TABLE series;", TxMode.NONE).execute()).join();

        client.close();
    }

    @Test
    void testExecute() {
        YdbQuery query = mock(YdbQuery.class);
        QuerySession querySession = mock(QuerySession.class);
        QueryStream queryStream = mock(QueryStream.class);
        QueryStream.PartsHandler mockPartsHandler = mock(QueryStream.PartsHandler.class);

        when(querySession.createQuery(anyString(), eq(TxMode.NONE))).thenReturn(queryStream);

        YdbDDLStatement statement = new YdbDDLStatement(query, querySession);

        // Simulate query execution by mocking the behavior of QueryStream
        QueryResultPart mockResultPart = mock(QueryResultPart.class);
        doAnswer(invocation -> {
            FluxSink<YdbResult> sink = invocation.getArgument(0);
            sink.next(new YdbResult(mockResultPart.getResultSetReader(), true));
            sink.complete();
            return null;
        }).when(queryStream).execute(eq(mockPartsHandler));

        // Add a subscriber
        LoggingQueryResultSubscriber subscriber = new LoggingQueryResultSubscriber();
        statement.addSubscriber(subscriber);

        // Execute and verify the result
        Flux<YdbResult> resultFlux = statement.execute();
        StepVerifier.create(resultFlux)
                .expectNextMatches(Objects::nonNull)
                .verifyComplete();

        verify(querySession).createQuery(anyString(), eq(TxMode.NONE));
        verify(queryStream).execute(eq(mockPartsHandler));
    }

    @Test
    void testUnsupportedOperations() {
        YdbQuery query = mock(YdbQuery.class);
        QuerySession querySession = mock(QuerySession.class);
        YdbDDLStatement statement = new YdbDDLStatement(query, querySession);

        // Assert that unsupported operations throw an exception
        assertThrows(UnsupportedOperationException.class, statement::add);
        assertThrows(UnsupportedOperationException.class, () -> statement.bind(0, "value"));
        assertThrows(UnsupportedOperationException.class, () -> statement.bind("key", "value"));
        assertThrows(UnsupportedOperationException.class, () -> statement.bindNull(0, String.class));
        assertThrows(UnsupportedOperationException.class, () -> statement.bindNull("key", String.class));
    }
}
