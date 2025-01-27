/*
 * Copyright 2022 YANDEX LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class YdbDMLStatement extends YdbStatement {
    private final QuerySession querySession;
    private final List<QueryResultSubscriber> subscribers = new ArrayList<>();

    public YdbDMLStatement(YdbQuery query, QuerySession querySession) {
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
    public Flux<YdbResult> execute() {
        Binding currentBinding = bindings.getCurrent();
        currentBinding.validate();

        String yql = query.getYqlQuery(currentBinding);

        try {
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
