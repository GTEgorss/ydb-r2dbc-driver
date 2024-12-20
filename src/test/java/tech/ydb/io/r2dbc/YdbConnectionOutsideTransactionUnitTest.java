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

package tech.ydb.io.r2dbc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.io.r2dbc.query.OperationType;
import tech.ydb.io.r2dbc.result.YdbResult;
import tech.ydb.io.r2dbc.settings.YdbTxSettings;
import tech.ydb.io.r2dbc.state.OutsideTransactionState;
import tech.ydb.io.r2dbc.state.CloseState;
import tech.ydb.io.r2dbc.state.InsideTransactionState;
import tech.ydb.io.r2dbc.state.YdbConnectionState;
import tech.ydb.proto.ValueProtos;
import tech.ydb.proto.table.YdbTable;
import tech.ydb.table.Session;
import tech.ydb.table.impl.PooledTableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.transaction.Transaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * @author Egor Kuleshov
 */
public class YdbConnectionOutsideTransactionUnitTest {
    private final PooledTableClient client = mock(PooledTableClient.class);
    private final YdbContext ydbContext = new YdbContext(client, OperationsConfig.defaultConfig());

    @Test
    public void executeSchemeQueryTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(0L)
                .verifyComplete();
        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeSchemeQuery(eq("test"), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeSchemeQueryCancelTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.SUCCESS));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .thenCancel()
                .verify();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeSchemeQuery(eq("test"), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();
        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryCancelTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .thenCancel()
                .verify();
        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryErrorTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.fail(Status.of(StatusCode.CANCELLED))
        ));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }


    @Test
    public void executeDataQueryErrorCancelTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.fail(Status.of(StatusCode.ABORTED))
        ));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .thenCancel()
                .verify();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryExceptionTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenThrow(new RuntimeException());

        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryExceptionCancelTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenThrow(new RuntimeException());

        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .thenCancel()
                .verify();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void executeDataQueryWithTxTest() {
        Session session = mock(Session.class);
        when(session.executeDataQuery(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(
                Result.success(new DataQueryResult(
                                YdbTable.ExecuteQueryResult.newBuilder()
                                        .setTxMeta(YdbTable.TransactionMeta.newBuilder()
                                                .setId("test_tx_id")
                                                .build())
                                        .addResultSets(ValueProtos.ResultSet
                                                .newBuilder()
                                                .getDefaultInstanceForType())
                                        .build()
                        )
                )
        ));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeDataQuery("test", Params.empty(), List.of(OperationType.SELECT))
                .flatMap(io.r2dbc.spi.Result::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(-1L)
                .verifyComplete();
        Assertions.assertEquals(new InsideTransactionState(ydbContext,
                        "test_tx_id",
                        session,
                        state.getYdbTxSettings().withAutoCommit(false)),
                queryExecutor.getCurrentState());
        Mockito.verify(session).executeDataQuery(eq("test"), eq(ydbTxSettings.txControl()), eq(Params.empty()), any());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void beginTransactionTest() {
        Session session = mock(Session.class);
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        Transaction transaction = Mockito.mock(Transaction.class);
        when(transaction.getId()).thenReturn("test_tx_id");
        when(session.beginTransaction(any(Transaction.Mode.class), any())).thenReturn(CompletableFuture.completedFuture(Result.success(transaction)));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        Mono<Void> beginTransactionMono = queryExecutor.beginTransaction();

        Assertions.assertEquals(OutsideTransactionState.class, queryExecutor.getCurrentState().getClass());

        beginTransactionMono.as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(InsideTransactionState.class, queryExecutor.getCurrentState().getClass());
        Assertions.assertEquals(new InsideTransactionState(ydbContext, "test_tx_id", session, ydbTxSettings),
                queryExecutor.getCurrentState());
        Mockito.verify(session).beginTransaction(eq(ydbTxSettings.getMode()), any());
        Mockito.verify(session, never()).close();
    }

    @Test
    public void beginTransactionFailTest() {
        Session session = mock(Session.class);
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));
        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();
        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        Transaction transaction = Mockito.mock(Transaction.class);
        when(transaction.getId()).thenReturn("test_tx_id");
        when(session.beginTransaction(any(Transaction.Mode.class), any()))
                .thenReturn(CompletableFuture.completedFuture(Result.fail(Status.of(StatusCode.ABORTED))));
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.beginTransaction()
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
        Mockito.verify(session).beginTransaction(eq(ydbTxSettings.getMode()), any());
        Mockito.verify(session).close();
    }

    @Test
    public void commitTransactionTest() {
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.commitTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
    }

    @Test
    public void rollbackTransactionTest() {
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.rollbackTransaction()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
    }

    @Test
    public void setAutoCommitTrueTest() {
        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.setAutoCommit(true)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(state, queryExecutor.getCurrentState());
    }

    @Test
    public void setAutoCommitFalseTest() {
        YdbTxSettings ydbTxSettings = ydbContext.getDefaultYdbTxSettings();

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.setAutoCommit(false)
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(new OutsideTransactionState(ydbContext, state.getYdbTxSettings().withAutoCommit(false)),
                queryExecutor.getCurrentState());
    }

    @Test
    public void closeTest() {
        YdbTxSettings ydbTxSettings = Mockito.mock(YdbTxSettings.class);

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbTxSettings);
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.close()
                .as(StepVerifier::create)
                .verifyComplete();

        Assertions.assertEquals(CloseState.INSTANCE, queryExecutor.getCurrentState());
    }

    @Test
    public void createSessionErrorTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.BAD_REQUEST)));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
    }

    @Test
    public void executeSchemeQueryFailTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenReturn(CompletableFuture.completedFuture(Status.of(StatusCode.BAD_REQUEST)));
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .map(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .verifyError(UnexpectedResultException.class);
        Mockito.verify(session).close();
    }

    @Test
    public void executeSchemeQueryErrorTest() {
        Session session = mock(Session.class);
        when(session.executeSchemeQuery(any(), any())).thenThrow(new RuntimeException());
        when(client.createSession(any())).thenReturn(CompletableFuture.completedFuture(Result.success(session)));

        YdbConnectionState state = new OutsideTransactionState(ydbContext, ydbContext.getDefaultYdbTxSettings());
        YdbConnection queryExecutor = new YdbConnection(ydbContext, state);

        queryExecutor.executeSchemeQuery("test")
                .as(StepVerifier::create)
                .verifyError(RuntimeException.class);
        Mockito.verify(session).close();
    }
}
