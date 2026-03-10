<?php

declare(strict_types=1);

namespace vennv\vosaka\libsql;

use vosaka\foroutines\LazyDeferred;

/**
 * AsyncDatabaseConnection — contract for a single non-blocking DB connection.
 *
 * All methods return LazyDeferred — call ->await() inside a Fiber context.
 */
interface AsyncDatabaseConnection {
	public function connect(): LazyDeferred;

	/** SELECT-style — resolves to QueryResult */
	public function query(string $sql, array $params = []): LazyDeferred;

	/** INSERT / UPDATE / DELETE — resolves to ExecuteResult */
	public function execute(string $sql, array $params = []): LazyDeferred;

	/** Lightweight health check — resolves to bool */
	public function ping(): LazyDeferred;

	/** Graceful close */
	public function close(): LazyDeferred;

	public function isConnected(): bool;
}
