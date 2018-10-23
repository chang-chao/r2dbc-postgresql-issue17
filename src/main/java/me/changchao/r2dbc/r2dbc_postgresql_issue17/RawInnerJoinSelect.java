package me.changchao.r2dbc.r2dbc_postgresql_issue17;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import io.r2dbc.client.R2dbc;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import reactor.core.publisher.Flux;

public class RawInnerJoinSelect {

	public static void main(String... args) throws InterruptedException {
		PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder().host("localhost")
				.database("test").username("postgres").password("postgres").build();

		R2dbc r2dbc = new R2dbc(new PostgresqlConnectionFactory(configuration));
		r2dbc.inTransaction(h -> {

			Flux<Integer> createTable = h.execute(
					"CREATE TABLE  IF NOT EXISTS mapping_error_test (id SERIAL PRIMARY KEY, short_val smallint)");
			Flux<Integer> createdata = h.execute("INSERT INTO mapping_error_test(short_val) VALUES (1)");
			return createTable.concatWith(createdata);

		}).blockLast();

		CountDownLatch latch = new CountDownLatch(2);
		r2dbc.withHandle(handle -> handle.select("select * from mapping_error_test")
				.mapResult(result -> result.map((rw, rm) -> rw.get("short_val", Integer.class)))
				.onErrorResume((Function<Throwable, Publisher<Integer>>) throwable -> {
					System.out.println("1");
					latch.countDown();
					throw new IllegalArgumentException();
				})).doOnError(throwable -> {
					System.out.println("2");
					latch.countDown();
				}).subscribe();

		latch.await();
	}
}
