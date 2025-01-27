package tech.ydb.io.r2dbc.subscribers;

import tech.ydb.query.result.QueryResultPart;

/**
 * Represents a subscriber that processes QueryResultPart objects.
 */
public interface QueryResultSubscriber {
    /**
     * Processes a single QueryResultPart.
     *
     * @param queryResultPart The query result part to process.
     */
    void onNext(QueryResultPart queryResultPart);
}
