<?php

declare(strict_types=1);

namespace vennv\vosaka\libsql;

use vosaka\foroutines\Async;
use vosaka\foroutines\Pause;

/**
 * AsyncPgsqlPool — Connection pool for AsyncPgsqlConnection.
 *
 * Mirrors AsyncMysqlPool exactly in behaviour — only difference is it
 * holds AsyncPgsqlConnection instances and wraps transactions in
 * PostgreSQL-style BEGIN / COMMIT / ROLLBACK.
 *
 * Usage:
 *
 *     $pool = new AsyncPgsqlPool(
 *         host: '127.0.0.1', port: 5432,
 *         user: 'postgres', password: 'secret', database: 'mydb',
 *         maxConnections: 10,
 *     );
 *
 *     $pool->warmUp(5)->await();
 *
 *     $result = $pool->query('SELECT * FROM users WHERE id = $1', [1])->await();
 *     $exec   = $pool->execute('UPDATE users SET name = $1 WHERE id = $2', ['Nam', 1])->await();
 *
 *     $pool->transaction(function (AsyncDatabaseConnection $conn): void {
 *         $conn->execute('INSERT INTO orders (user_id) VALUES ($1)', [42])->await();
 *         $conn->execute('UPDATE users SET order_count = order_count + 1 WHERE id = $1', [42])->await();
 *     })->await();
 *
 *     $pool->closeAll()->await();
 */
final class AsyncPgsqlPool implements AsyncDatabasePool {
	/** @var AsyncPgsqlConnection[] */
	private array $idle  = [];

	/** @var array<int, AsyncPgsqlConnection> keyed by spl_object_id */
	private array $inUse = [];

	private int $totalCreated = 0;

	public function __construct(
		private readonly string $host,
		private readonly int    $port,
		private readonly string $user,
		private readonly string $password,
		private readonly string $database,
		private readonly int    $maxConnections  = 10,
		private readonly float  $connectTimeout  = 10.0,
	) {
	}

	public function warmUp(int $count): Async {
		return Async::new(function () use ($count): void {
			$toCreate = min($count, $this->maxConnections)
				- count($this->idle)
				- count($this->inUse);

			for ($i = 0; $i < $toCreate; $i++) {
				$this->idle[] = $this->createConnection();
			}
		});
	}

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
	 * Run a callback inside a PostgreSQL transaction.
	 *
	 * The connection passed to $callback is the raw AsyncPgsqlConnection —
	 * type-hinted as AsyncDatabaseConnection so code stays driver-agnostic.
	 *
	 * Uses BEGIN / COMMIT / ROLLBACK (standard SQL, same as MySQL
	 * START TRANSACTION / COMMIT / ROLLBACK in semantics).
	 */
	public function transaction(callable $callback): Async {
		return Async::new(function () use ($callback): mixed {
			$conn = $this->acquire();
			try {
				$conn->execute('BEGIN')->await();
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
			$this->idle         = [];
			$this->inUse        = [];
			$this->totalCreated = 0;
		});
	}

	public function stats(): array {
		return [
			'idle'    => count($this->idle),
			'in_use'  => count($this->inUse),
			'total'   => $this->totalCreated,
			'max'     => $this->maxConnections,
		];
	}

	/**
	 * Acquire a connection from the pool.
	 *
	 * Priority:
	 *   1. Reuse an idle connection (stream still alive)
	 *   2. Create a new connection if under limit
	 *   3. Cooperatively suspend via Pause::new() until one is released
	 */
	private function acquire(): AsyncPgsqlConnection {
		// Prefer idle — discard stale ones
		while (!empty($this->idle)) {
			$conn = array_pop($this->idle);
			if ($conn->isConnected()) {
				$this->inUse[spl_object_id($conn)] = $conn;
				return $conn;
			}
			$this->totalCreated--;
		}

		// Under limit — spin up a fresh connection
		if ($this->totalCreated < $this->maxConnections) {
			$conn = $this->createConnection();
			$this->inUse[spl_object_id($conn)] = $conn;
			return $conn;
		}

		// Pool exhausted — yield cooperatively
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

	private function release(AsyncPgsqlConnection $conn): void {
		$id = spl_object_id($conn);
		unset($this->inUse[$id]);

		if ($conn->isConnected()) {
			$this->idle[] = $conn;
		} else {
			$this->totalCreated--;
		}
	}

	private function createConnection(): AsyncPgsqlConnection {
		$conn = new AsyncPgsqlConnection(
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
