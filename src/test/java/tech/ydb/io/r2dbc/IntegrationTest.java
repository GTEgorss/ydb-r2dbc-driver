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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tech.ydb.io.r2dbc.result.YdbResult;

/**
 * @author Egor Kuleshov
 */
public class IntegrationTest extends IntegrationBaseTest {
    @BeforeEach
    public void setUp() {
        createTable();
    }

    @AfterEach
    public void cleanUp() {
        dropTable();
    }

    @Test
    public void createAndDropTable() {
    }

    @Test
    public void doubleSelectTable() {
        upsertData(r2dbc.connection())
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .expectNext(1L)
                .verifyComplete();

        r2dbc.connection().createStatement(
                        "select * from t1 order by id asc;" +
                                "select * from t1 order by id desc;")
                .execute()
                .flatMap(ydbResult -> ydbResult.map((row, rowMetadata) -> row.get("id")))
                .as(StepVerifier::create)
                .expectNext(123)
                .expectNext(124)
                .expectNext(124)
                .expectNext(123)
                .verifyComplete();
    }

    @Test
    public void doubleSelectAndUpsertTable() {
        upsertData(r2dbc.connection())
                .flatMap(YdbResult::getRowsUpdated)
                .as(StepVerifier::create)
                .expectNext(1L)
                .expectNext(1L)
                .verifyComplete();

        r2dbc.connection().createStatement(
                        "select * from t1 order by id asc;" +
                                "upsert into t1 (id, value) values (125, 'test');" +
                                "select * from t1 order by id desc;")
                .execute()
                .as(StepVerifier::create)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    public void upsertAndSelectTable() {
        upsertData(r2dbc.connection())
                .thenMany(r2dbc.connection().createStatement("select * from t1 order by id asc;")
                        .execute())
                .flatMap(ydbResult -> ydbResult.map((row, rowMetadata) -> row.get("id")))
                .as(StepVerifier::create)
                .expectNext(123)
                .expectNext(124)
                .verifyComplete();
    }

    private static Flux<YdbResult> upsertData(YdbConnection connection) {
        return connection.createStatement("upsert into t1 (id, value) values (?, ?);")
                .bind(0, 123)
                .bind(1, "test_1")
                .add()
                .bind(0, 124)
                .bind(1, "test_2")
                .execute();
    }
}