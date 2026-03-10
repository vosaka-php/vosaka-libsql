<?php

declare(strict_types=1);

namespace vennv\vosaka\libsql;

use vosaka\foroutines\Async;

/**
 * AsyncDatabasePool — contract for a connection pool.
 *
 * All methods return Async — call ->await() inside a Fiber context.
 */
interface AsyncDatabasePool {
	/** Pre-warm $count connections. */
	public function warmUp(int $count): Async;

	/** Run a SELECT query on a pooled connection — resolves to QueryResult. */
	public function query(string $sql, array $params = []): Async;

	/** Run INSERT/UPDATE/DELETE on a pooled connection — resolves to ExecuteResult. */
	public function execute(string $sql, array $params = []): Async;

	/**
	 * Run multiple statements inside a transaction.
	 *
	 *     $pool->transaction(function(AsyncDatabaseConnection $conn): mixed {
	 *         $conn->execute('INSERT INTO ...', [...])->await();
	 *         return $conn->query('SELECT ...')->await();
	 *     })->await();
	 */
	public function transaction(callable $callback): Async;

	/** Close all idle + in-use connections. */
	public function closeAll(): Async;

	/**
	 * @return array{idle: int, in_use: int, total: int, max: int}
	 */
	public function stats(): array;
}
