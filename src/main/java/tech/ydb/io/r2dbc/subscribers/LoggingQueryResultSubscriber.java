package tech.ydb.io.r2dbc.subscribers;

import tech.ydb.query.result.QueryResultPart;

/**
 * A subscriber that logs the received QueryResultPart objects.
 */
public class LoggingQueryResultSubscriber implements QueryResultSubscriber {
    @Override
    public void onNext(QueryResultPart queryResultPart) {
        System.out.println("Received QueryResultPart: " + queryResultPart.toString());
    }
}
