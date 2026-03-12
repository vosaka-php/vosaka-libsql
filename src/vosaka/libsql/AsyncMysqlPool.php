<?php

declare(strict_types=1);

namespace vosaka\libsql;

use vosaka\foroutines\Async;
use vosaka\foroutines\Pause;

/**
 * AsyncMysqlPool — Connection pool for AsyncMysqlConnection.
 *
 * Maintains a fixed pool of connections. When all connections are in use,
 * the calling Fiber suspends cooperatively (via Pause::new) until one
 * is released — no busy spinning, no blocking.
 *
 * Usage:
 *
 *     $pool = new AsyncMysqlPool(
 *         host: '127.0.0.1', port: 3306,
 *         user: 'root', password: 'secret', database: 'mydb',
 *         maxConnections: 10,
 *     );
 *
 *     // Warm up connections (optional but reduces first-query latency)
 *     $pool->warmUp(5)->await();
 *
 *     // Use inside Launch::new or Async::new:
 *     $result = $pool->query('SELECT * FROM users WHERE id = ?', [1])->await();
 *     $exec   = $pool->execute('UPDATE users SET name = ? WHERE id = ?', ['Nam', 1])->await();
 *
 *     $pool->closeAll()->await();
 */
final class AsyncMysqlPool implements AsyncDatabasePool {
	/** @var AsyncMysqlConnection[] Idle connections available for use */
	private array $idle = [];

	/** @var array<int, AsyncMysqlConnection> Connections currently in use (keyed by spl_object_id) */
	private array $inUse = [];

	private int $totalCreated = 0;

	public function __construct(
		private readonly string $host,
		private readonly int $port,
		private readonly string $user,
		private readonly string $password,
		private readonly string $database,
		private readonly int $maxConnections = 10,
		private readonly float $connectTimeout = 10.0,
	) {
	}

	/**
	 * Pre-warm $count connections so they're ready before the first query.
	 */
	public function warmUp(int $count): Async {
		return Async::new(function () use ($count): void {
			$toCreate = min($count, $this->maxConnections) - count($this->idle) - count($this->inUse);
			for ($i = 0; $i < $toCreate; $i++) {
				$this->idle[] = $this->createConnection();
			}
		});
	}

	/**
	 * Run a SELECT query using a pooled connection.
	 */
	public function query(string $sql, array $params = []): Async {
		return Async::new(function () use ($sql, $params): QueryResult {
			$conn = $this->acquire();
			try {
				return $conn->query($sql, $params)->await();
			} finally {
				$this->release($conn);
			}
		});
	}

	/**
	 * Run an INSERT/UPDATE/DELETE using a pooled connection.
	 */
	public function execute(string $sql, array $params = []): Async {
		return Async::new(function () use ($sql, $params): ExecuteResult {
			$conn = $this->acquire();
			try {
				return $conn->execute($sql, $params)->await();
			} finally {
				$this->release($conn);
			}
		});
	}

	/**
	 * Run multiple queries in a transaction.
	 *
	 * Example:
	 *   $pool->transaction(function(AsyncMysqlConnection $conn): mixed {
	 *       $conn->execute('INSERT INTO ...', [...])->await();
	 *       return $conn->query('SELECT ...')->await();
	 *   })->await();
	 */
	public function transaction(callable $callback): Async {
		return Async::new(function () use ($callback): mixed {
			$conn = $this->acquire();
			try {
				$conn->execute('START TRANSACTION')->await();
				$result = $callback($conn);
				$conn->execute('COMMIT')->await();
				return $result;
			} catch (\Throwable $e) {
				try {
					$conn->execute('ROLLBACK')->await();
				} catch (\Throwable) {
				}
				throw $e;
			} finally {
				$this->release($conn);
			}
		});
	}

	/**
	 * Close all connections (idle + in-use).
	 */
	public function closeAll(): Async {
		return Async::new(function (): void {
			foreach ($this->idle as $conn) {
				try {
					$conn->close()->await();
				} catch (\Throwable) {
				}
			}
			foreach ($this->inUse as $conn) {
				try {
					$conn->close()->await();
				} catch (\Throwable) {
				}
			}
			$this->idle = [];
			$this->inUse = [];
			$this->totalCreated = 0;
		});
	}

	public function stats(): array {
		return [
			'idle' => count($this->idle),
			'in_use' => count($this->inUse),
			'total' => $this->totalCreated,
			'max' => $this->maxConnections,
		];
	}

	/**
	 * Acquire a connection from the pool.
	 *
	 * If an idle connection is available, return it immediately.
	 * If under the max limit, create a new one.
	 * Otherwise, cooperatively suspend via Pause::new() until one is released.
	 */
	private function acquire(): AsyncMysqlConnection {
		// Try idle connections first — no ping, just stream check
		while (!empty($this->idle)) {
			$conn = array_pop($this->idle);
			if ($conn->isConnected()) {
				$this->inUse[spl_object_id($conn)] = $conn;
				return $conn;
			}
			$this->totalCreated--;
		}

		// Under the limit — create a new connection
		if ($this->totalCreated < $this->maxConnections) {
			$conn = $this->createConnection();
			$this->inUse[spl_object_id($conn)] = $conn;
			return $conn;
		}

		// Pool exhausted — yield cooperatively until a connection is released
		if (\Fiber::getCurrent() !== null) {
			while (empty($this->idle)) {
				Pause::new();
			}
		} else {
			while (empty($this->idle)) {
				usleep(500);
			}
		}

		// Non-recursive retry to avoid stack overflow under heavy load
		$conn = array_pop($this->idle);
		if ($conn !== null && $conn->isConnected()) {
			$this->inUse[spl_object_id($conn)] = $conn;
			return $conn;
		}
		$this->totalCreated--;
		return $this->acquire();
	}

	private function release(AsyncMysqlConnection $conn): void {
		$id = spl_object_id($conn);
		unset($this->inUse[$id]);

		if ($conn->isConnected()) {
			$this->idle[] = $conn;
		} else {
			// Connection died during query — remove from pool count
			$this->totalCreated--;
		}
	}

	private function createConnection(): AsyncMysqlConnection {
		$conn = new AsyncMysqlConnection(
			$this->host,
			$this->port,
			$this->user,
			$this->password,
			$this->database,
			$this->connectTimeout,
		);

		$conn->connect()->await(); // Fiber suspends here until TCP + handshake done
		$this->totalCreated++;

		return $conn;
	}
}
