package tech.ydb.io.r2dbc.statement;

import reactor.core.publisher.FluxSink;
import tech.ydb.core.Issue;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.subscribers.QueryResultSubscriber;
import tech.ydb.query.QueryStream;
import tech.ydb.query.result.QueryResultPart;

import java.util.List;

/**
 * Handles QueryResultParts from QueryStream and notifies subscribers.
 */
public class QueryPartsHandler implements QueryStream.PartsHandler {

    private final List<QueryResultSubscriber> subscribers;
    private final FluxSink<YdbResult> sink;

    public QueryPartsHandler(List<QueryResultSubscriber> subscribers, FluxSink<YdbResult> sink) {
        this.subscribers = subscribers;
        this.sink = sink;
    }

    @Override
    public void onNextPart(QueryResultPart part) {
        try {
            for (QueryResultSubscriber subscriber : subscribers) {
                subscriber.onNext(part);
            }

            YdbResult ydbResult = convertToYdbResult(part);
            sink.next(ydbResult);
        } catch (Exception e) {
            sink.error(e);
        }
    }

    @Override
    public void onIssues(Issue[] issues) {
        for (Issue issue : issues) {
            sink.error(new RuntimeException("Query issue: " + issue.getMessage()));
        }
    }

    /**
     * Converts a QueryResultPart into a YdbResult.
     *
     * @param part The QueryResultPart to convert.
     * @return The resulting YdbResult.
     */
    private YdbResult convertToYdbResult(QueryResultPart part) {
        return new YdbResult(part.getResultSetReader(), true);
    }
}
