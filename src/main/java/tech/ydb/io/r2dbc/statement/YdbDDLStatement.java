package tech.ydb.io.r2dbc.statement;

import reactor.core.publisher.Flux;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.io.r2dbc.query.YdbQuery;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.statement.binding.Binding;
import tech.ydb.io.r2dbc.subscribers.QueryResultSubscriber;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Egor Kuleshov
 */
public class YdbDDLStatement extends YdbStatement {
    private static final String NOT_SUPPORTED_MESSAGE = "Operation not supported for YdbDDLStatement";

    private final QuerySession querySession;
    private final List<QueryResultSubscriber> subscribers = new ArrayList<>();

    public YdbDDLStatement(YdbQuery query, QuerySession querySession) {
        super(query, null);
        this.querySession = querySession;
    }

    /**
     * Adds a subscriber that will process QueryResultParts of the QueryStream.
     *
     * @param subscriber The subscriber to be added.
     */
    public void addSubscriber(QueryResultSubscriber subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public YdbStatement add() {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bind(int i, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bind(String s, Object o) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bindNull(int i, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public YdbStatement bindNull(String s, Class<?> aClass) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public Flux<YdbResult> execute() {
        try {
            String yql = query.getYqlQuery(Binding.empty());

            QueryStream queryStream = querySession.createQuery(yql, TxMode.NONE);

            return Flux.create(sink -> {
                QueryPartsHandler handler = new QueryPartsHandler(subscribers, sink);
                queryStream.execute(handler)
                        .thenAccept(result -> sink.complete())
                        .exceptionally(e -> {
                            sink.error(e);
                            return null;
                        });
            });

        } catch (Exception e) {
            return Flux.error(e);
        }
    }

}
