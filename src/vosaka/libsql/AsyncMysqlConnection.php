<?php

declare(strict_types=1);

namespace vosaka\libsql;

use vosaka\foroutines\AsyncIO;
use vosaka\foroutines\LazyDeferred;

/**
 * AsyncMysqlConnection — Non-blocking MySQL client built on vosaka-foroutines AsyncIO.
 *
 * Usage (inside Launch::new or Async::new):
 *
 *     $conn = new AsyncMysqlConnection('127.0.0.1', 3306, 'root', 'secret', 'mydb');
 *     $conn->connect()->await();
 *
 *     $result = $conn->query('SELECT * FROM users WHERE id = ?', [1])->await();
 *     foreach ($result->rows as $row) { ... }
 *
 *     $result = $conn->execute('INSERT INTO users (name) VALUES (?)', ['Nam'])->await();
 *     echo $result->lastInsertId;
 *
 *     $conn->close()->await();
 *
 * All public methods return LazyDeferred — call ->await() to execute.
 * Must be called from within a Fiber context (Launch::new / Async::new).
 */
final class AsyncMysqlConnection implements AsyncDatabaseConnection {
	/** @var resource|null */
	private mixed $socket = null;

	private int $sequenceId = 0;

	// MySQL capability flags we request
	private const CLIENT_LONG_PASSWORD = 0x00000001;
	private const CLIENT_FOUND_ROWS = 0x00000002;
	private const CLIENT_LONG_FLAG = 0x00000004;
	private const CLIENT_CONNECT_WITH_DB = 0x00000008;
	private const CLIENT_PROTOCOL_41 = 0x00000200;
	private const CLIENT_SECURE_CONNECTION = 0x00008000;
	private const CLIENT_PLUGIN_AUTH = 0x00080000;

	public function __construct(
		private readonly string $host,
		private readonly int    $port,
		private readonly string $user,
		private readonly string $password,
		private readonly string $database,
		private readonly float  $timeoutSeconds = 10.0,
	) {
	}

	/**
	 * Connect to MySQL and perform handshake/auth.
	 * Must be called before any query/execute.
	 */
	public function connect(): LazyDeferred {
		return new LazyDeferred(function (): void {
			$this->socket = $this->doBlockingTcpConnect();
			$this->doHandshake();
		});
	}

	/**
	 * Open a TCP connection using a SHORT blocking connect, then immediately
	 * switch to non-blocking for all subsequent I/O.
	 *
	 * Why not AsyncIO::tcpConnect()?
	 * AsyncIO uses STREAM_CLIENT_ASYNC_CONNECT which creates a non-blocking
	 * socket. After the connect completes, feof() can transiently return true
	 * before the server sends the greeting packet, causing pollOnce() to
	 * resume the read watcher with false (EOF signal). This makes readExact()
	 * receive an empty string and throw immediately.
	 *
	 * Using a short blocking connect avoids this: by the time stream_socket_client
	 * returns, the TCP handshake is fully done and we can safely switch to
	 * non-blocking mode for the rest of the session.
	 */
	private function doBlockingTcpConnect(): mixed {
		$address = "tcp://{$this->host}:{$this->port}";
		$errno   = 0;
		$errstr  = '';

		// Blocking connect — short timeout (connect phase only)
		$socket = @stream_socket_client(
			$address,
			$errno,
			$errstr,
			$this->timeoutSeconds,
			STREAM_CLIENT_CONNECT, // blocking, not ASYNC_CONNECT
		);

		if ($socket === false) {
			throw new \RuntimeException(
				"Failed to connect to MySQL at {$address}: [{$errno}] {$errstr}"
			);
		}

		// Switch to non-blocking for all subsequent async I/O
		stream_set_blocking($socket, false);

		return $socket;
	}

	/**
	 * Execute a SELECT-style query and return rows + field metadata.
	 *
	 * @param string  $sql    SQL with ? placeholders
	 * @param array   $params Values to interpolate
	 * @return LazyDeferred Resolves to QueryResult
	 */
	public function query(string $sql, array $params = []): LazyDeferred {
		return new LazyDeferred(function () use ($sql, $params): QueryResult {
			$this->assertConnected();
			$this->sequenceId = 0; // reset per-command sequence
			$sql = $this->interpolate($sql, $params);
			$this->sendPacket("\x03" . $sql); // COM_QUERY
			return $this->readQueryResponse();
		});
	}

	/**
	 * Execute an INSERT/UPDATE/DELETE statement.
	 *
	 * @param string  $sql    SQL with ? placeholders
	 * @param array   $params Values to interpolate
	 * @return LazyDeferred Resolves to ExecuteResult
	 */
	public function execute(string $sql, array $params = []): LazyDeferred {
		return new LazyDeferred(function () use ($sql, $params): ExecuteResult {
			$this->assertConnected();
			$this->sequenceId = 0; // reset per-command sequence
			$sql = $this->interpolate($sql, $params);
			$this->sendPacket("\x03" . $sql); // COM_QUERY
			return $this->readExecuteResponse();
		});
	}

	/**
	 * Send COM_PING to check connection health.
	 *
	 * @return LazyDeferred Resolves to bool (true = alive)
	 */
	public function ping(): LazyDeferred {
		return new LazyDeferred(function (): bool {
			if ($this->socket === null) {
				return false;
			}
			try {
				$this->sequenceId = 0;
				$this->sendPacket("\x0e"); // COM_PING
				$packet = $this->readPacket();
				return ord($packet[0]) === 0x00; // OK packet
			} catch (\RuntimeException) {
				return false;
			}
		});
	}

	/**
	 * Close the connection gracefully (COM_QUIT).
	 */
	public function close(): LazyDeferred {
		return new LazyDeferred(function (): void {
			if ($this->socket === null) {
				return;
			}
			try {
				$this->sequenceId = 0;
				$this->sendPacket("\x01"); // COM_QUIT
			} catch (\RuntimeException) {
				// ignore write errors on close
			} finally {
				fclose($this->socket);
				$this->socket = null;
			}
		});
	}

	public function isConnected(): bool {
		return $this->socket !== null && is_resource($this->socket);
	}

	private function doHandshake(): void {
		// 1. Read server greeting (Initial Handshake Packet)
		$greeting = $this->readPacket();
		$this->sequenceId = 0;

		$offset = 0;

		// protocol version (1 byte)
		$protocolVersion = ord($greeting[$offset++]);
		if ($protocolVersion !== 10) {
			throw new \RuntimeException(
				"Unsupported MySQL protocol version: {$protocolVersion}"
			);
		}

		// server version (null-terminated string)
		$nullPos = strpos($greeting, "\x00", $offset);
		// $serverVersion = substr($greeting, $offset, $nullPos - $offset);
		$offset = $nullPos + 1;

		// connection id (4 bytes LE)
		$offset += 4;

		// auth-plugin-data part 1 (8 bytes)
		$authData1 = substr($greeting, $offset, 8);
		$offset += 8;

		// filler (1 byte)
		$offset += 1;

		// capability flags lower 2 bytes
		$capLow = unpack('v', substr($greeting, $offset, 2))[1];
		$offset += 2;

		// character set (1 byte)
		$offset += 1;

		// status flags (2 bytes)
		$offset += 2;

		// capability flags upper 2 bytes
		$capHigh = unpack('v', substr($greeting, $offset, 2))[1];
		$offset += 2;

		$capabilities = $capLow | ($capHigh << 16);

		// auth-plugin-data-len (1 byte)
		$authDataLen = ord($greeting[$offset++]);

		// reserved (10 bytes)
		$offset += 10;

		// auth-plugin-data part 2
		$part2Len = max(13, $authDataLen - 8);
		$authData2 = substr($greeting, $offset, $part2Len - 1); // -1 strips null terminator
		$offset += $part2Len;

		$authPluginData = $authData1 . $authData2;

		// auth-plugin-name (null-terminated)
		$authPlugin = 'mysql_native_password';
		if ($capabilities & self::CLIENT_PLUGIN_AUTH) {
			$nullPos = strpos($greeting, "\x00", $offset);
			if ($nullPos !== false) {
				$authPlugin = substr($greeting, $offset, $nullPos - $offset);
			}
		}

		// 2. Compute auth response
		$authResponse = $this->computeAuthResponse($this->password, $authPluginData, $authPlugin);

		// 3. Send HandshakeResponse41 — must use sequence id 1
		// (server greeting was seq=0, our response must be seq=1)
		$this->sequenceId = 1;
		$this->sendHandshakeResponse($authResponse, $capabilities);

		// 4. Read OK / ERR / AUTH_SWITCH
		$response = $this->readPacket();
		$firstByte = ord($response[0]);

		if ($firstByte === 0xFE) {
			// Auth switch request — handle caching_sha2_password fast path
			$this->handleAuthSwitch($response, $authPluginData);
		} elseif ($firstByte === 0xFF) {
			throw new \RuntimeException('MySQL auth error: ' . $this->parseErrorMessage($response));
		}
		// 0x00 = OK
	}

	private function computeAuthResponse(
		string $password,
		string $authData,
		string $plugin,
	): string {
		if ($password === '') {
			return '';
		}

		return match ($plugin) {
			'mysql_native_password' => $this->nativePasswordAuth($password, $authData),
			'caching_sha2_password' => $this->sha2PasswordAuth($password, $authData),
			default => $this->nativePasswordAuth($password, $authData),
		};
	}

	/**
	 * mysql_native_password: SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
	 */
	private function nativePasswordAuth(string $password, string $salt): string {
		$hash1 = sha1($password, true);
		$hash2 = sha1($hash1, true);
		$combined = sha1($salt . $hash2, true);

		$result = '';
		for ($i = 0; $i < 20; $i++) {
			$result .= chr(ord($hash1[$i]) ^ ord($combined[$i]));
		}
		return $result;
	}

	/**
	 * caching_sha2_password fast auth path: XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + salt))
	 */
	private function sha2PasswordAuth(string $password, string $salt): string {
		$hash1 = hash('sha256', $password, true);
		$hash2 = hash('sha256', $hash1, true);
		$hash3 = hash('sha256', $hash2 . $salt, true);

		$result = '';
		$len = strlen($hash1);
		for ($i = 0; $i < $len; $i++) {
			$result .= chr(ord($hash1[$i]) ^ ord($hash3[$i]));
		}
		return $result;
	}

	private function sendHandshakeResponse(string $authResponse, int $serverCapabilities): void {
		$clientFlags = self::CLIENT_PROTOCOL_41
			| self::CLIENT_LONG_PASSWORD
			| self::CLIENT_LONG_FLAG
			| self::CLIENT_FOUND_ROWS
			| self::CLIENT_SECURE_CONNECTION;

		if ($this->database !== '') {
			$clientFlags |= self::CLIENT_CONNECT_WITH_DB;
		}

		if ($serverCapabilities & self::CLIENT_PLUGIN_AUTH) {
			$clientFlags |= self::CLIENT_PLUGIN_AUTH;
		}

		$packet = pack('V', $clientFlags)          // client flags (4 bytes)
			. pack('V', 16777216)                  // max packet size (4 bytes)
			. chr(45)                              // charset utf8mb4
			. str_repeat("\x00", 23)               // reserved (23 bytes)
			. $this->user . "\x00"                 // username (null-terminated)
			. chr(strlen($authResponse))           // auth response length (1 byte)
			. $authResponse                        // auth response
			. ($this->database !== '' ? $this->database . "\x00" : '')
			. 'mysql_native_password' . "\x00";    // auth plugin name

		$this->sendPacket($packet);
	}

	private function handleAuthSwitch(string $packet, string $originalSalt): void {
		// Parse: 0xFE + plugin_name\0 + plugin_data\0
		$offset = 1;
		$nullPos = strpos($packet, "\x00", $offset);
		$pluginName = substr($packet, $offset, $nullPos - $offset);
		$newSalt = substr($packet, $nullPos + 1, -1); // strip trailing null

		$authResponse = $this->computeAuthResponse($this->password, $newSalt, $pluginName);
		$this->sendPacket($authResponse);

		$response = $this->readPacket();
		if (ord($response[0]) === 0xFF) {
			throw new \RuntimeException('MySQL auth switch error: ' . $this->parseErrorMessage($response));
		}
	}

	private function readQueryResponse(): QueryResult {
		$packet = $this->readPacket();
		$firstByte = ord($packet[0]);

		if ($firstByte === 0xFF) {
			throw new \RuntimeException('MySQL query error: ' . $this->parseErrorMessage($packet));
		}

		if ($firstByte === 0x00) {
			// OK packet — no result set (e.g. CREATE TABLE)
			return new QueryResult([], []);
		}

		// Column count (length-encoded integer)
		$offset = 0;
		$columnCount = (int) $this->readLengthEncodedInt($packet, $offset);

		// Read column definition packets
		$fields = [];
		for ($i = 0; $i < $columnCount; $i++) {
			$fields[] = $this->parseColumnDefinition($this->readPacket());
		}

		// EOF packet after column definitions (protocol < 4.1 deprecates it but still sent)
		$eofOrOk = $this->readPacket();
		// 0xFE = EOF, 0x00 = OK (with CLIENT_DEPRECATE_EOF — we don't set that flag, so always EOF)

		// Read row data packets
		$rows = [];
		while (true) {
			$row = $this->readPacket();
			if (ord($row[0]) === 0xFE && strlen($row) < 9) {
				break; // EOF packet — end of result set
			}
			if (ord($row[0]) === 0xFF) {
				throw new \RuntimeException('MySQL row error: ' . $this->parseErrorMessage($row));
			}
			$rows[] = $this->parseTextRow($row, $fields);
		}

		return new QueryResult($rows, $fields);
	}

	private function readExecuteResponse(): ExecuteResult {
		$packet = $this->readPacket();
		$firstByte = ord($packet[0]);

		if ($firstByte === 0xFF) {
			throw new \RuntimeException('MySQL execute error: ' . $this->parseErrorMessage($packet));
		}

		if ($firstByte === 0x00) {
			// OK packet
			$offset = 1;
			$affectedRows = (int) $this->readLengthEncodedInt($packet, $offset);
			$lastInsertId = (int) $this->readLengthEncodedInt($packet, $offset);
			return new ExecuteResult($affectedRows, $lastInsertId > 0 ? $lastInsertId : null);
		}

		// Result set returned (e.g. INSERT ... RETURNING in MariaDB)
		// Fall back to treating it as query response
		$result = $this->readQueryResponse();
		return new ExecuteResult(0, null);
	}

	/**
	 * Read one MySQL packet.
	 * 4-byte header: 3 bytes length (LE) + 1 byte sequence id.
	 * Body: `length` bytes of payload.
	 *
	 * Uses AsyncIO::streamRead()->await() so the Fiber suspends
	 * cooperatively while waiting for data from the MySQL server.
	 */
	private function readPacket(): string {
		// Read header
		$header = $this->readExact(4);
		$b = unpack('C4', $header);
		$length = $b[1] | ($b[2] << 8) | ($b[3] << 16);
		$this->sequenceId = $b[4];

		if ($length === 0) {
			return '';
		}

		return $this->readExact($length);
	}

	/**
	 * Read exactly $n bytes from the MySQL socket.
	 *
	 * AsyncIO::streamRead() suspends the Fiber until data arrives, then reads
	 * up to $n bytes. It may return fewer (partial read), so we loop.
	 *
	 * Empty string = stream not ready on this poll tick, NOT necessarily EOF.
	 * True EOF is detected by feof(). We yield cooperatively and retry.
	 */
	private function readExact(int $n): string {
		$buffer = '';
		$remaining = $n;
		$emptyStreak = 0;
		$maxEmptyStreaks = 1000; // ~0.5s worth of retries

		while ($remaining > 0) {
			if (!is_resource($this->socket) || feof($this->socket)) {
				throw new \RuntimeException(
					"MySQL connection closed unexpectedly (expected {$n} bytes, got " . strlen($buffer) . ')'
				);
			}

			$chunk = AsyncIO::streamRead($this->socket, $remaining)->await();

			if ($chunk === '') {
				$emptyStreak++;
				if ($emptyStreak > $maxEmptyStreaks) {
					throw new \RuntimeException(
						"MySQL read timed out (expected {$n} bytes, got " . strlen($buffer) . ')'
					);
				}
				if (\Fiber::getCurrent() !== null) {
					\Fiber::suspend();
				} else {
					usleep(500);
				}
				continue;
			}

			$emptyStreak = 0;
			$buffer .= $chunk;
			$remaining -= strlen($chunk);
		}

		return $buffer;
	}

	/**
	 * Send one MySQL packet.
	 * Prepends 3-byte length + 1-byte sequence id, then writes via AsyncIO.
	 */
	private function sendPacket(string $payload): void {
		$length = strlen($payload);
		// Use current sequenceId (caller already set it to the right value),
		// then increment for the next packet in this command exchange.
		$header = chr($length & 0xFF)
			. chr(($length >> 8) & 0xFF)
			. chr(($length >> 16) & 0xFF)
			. chr($this->sequenceId & 0xFF);

		$this->sequenceId++;

		// AsyncIO::streamWrite suspends Fiber until all bytes are flushed
		AsyncIO::streamWrite($this->socket, $header . $payload)->await();
	}

	/**
	 * Parse a column definition packet (COM_QUERY result set metadata).
	 * Format: catalog / schema / table / org_table / name / org_name /
	 *         0x0c / charset / column_length / type / flags / decimals
	 */
	private function parseColumnDefinition(string $packet): ColumnDefinition {
		$offset = 0;

		$this->skipLengthEncodedString($packet, $offset); // catalog
		$this->skipLengthEncodedString($packet, $offset); // schema
		$this->skipLengthEncodedString($packet, $offset); // table (alias)
		$this->skipLengthEncodedString($packet, $offset); // org_table
		$name = $this->readLengthEncodedString($packet, $offset); // name (alias)
		$this->skipLengthEncodedString($packet, $offset); // org_name

		$offset += 1; // 0x0c filler
		$charset = unpack('v', substr($packet, $offset, 2))[1];
		$offset += 2;
		$columnLength = unpack('V', substr($packet, $offset, 4))[1];
		$offset += 4;
		$type = ord($packet[$offset++]);
		$flags = unpack('v', substr($packet, $offset, 2))[1];
		$offset += 2;
		$decimals = ord($packet[$offset]);

		return new ColumnDefinition($name, $type, $flags, $charset, $columnLength, $decimals);
	}

	/**
	 * Parse a text protocol result row.
	 * Each field is a length-encoded string (NULL = 0xFB).
	 *
	 * @param ColumnDefinition[] $fields
	 * @return array<string, string|null>
	 */
	private function parseTextRow(string $packet, array $fields): array {
		$offset = 0;
		$row = [];

		foreach ($fields as $field) {
			if (!isset($packet[$offset])) {
				$row[$field->name] = null;
				continue;
			}

			$firstByte = ord($packet[$offset]);

			if ($firstByte === 0xFB) {
				// NULL value
				$offset++;
				$row[$field->name] = null;
			} else {
				$row[$field->name] = $this->readLengthEncodedString($packet, $offset);
			}
		}

		return $row;
	}

	private function readLengthEncodedInt(string $data, int &$offset): int|string {
		$firstByte = ord($data[$offset++]);

		if ($firstByte < 0xFB) {
			return $firstByte;
		}

		if ($firstByte === 0xFC) {
			$val = unpack('v', substr($data, $offset, 2))[1];
			$offset += 2;
			return $val;
		}

		if ($firstByte === 0xFD) {
			$b = unpack('C3', substr($data, $offset, 3));
			$val = $b[1] | ($b[2] << 8) | ($b[3] << 16);
			$offset += 3;
			return $val;
		}

		// 0xFE — 8-byte int (large values)
		$val = unpack('P', substr($data, $offset, 8))[1];
		$offset += 8;
		return $val;
	}

	private function readLengthEncodedString(string $data, int &$offset): string {
		$length = (int) $this->readLengthEncodedInt($data, $offset);
		$str = substr($data, $offset, $length);
		$offset += $length;
		return $str;
	}

	private function skipLengthEncodedString(string $data, int &$offset): void {
		$length = (int) $this->readLengthEncodedInt($data, $offset);
		$offset += $length;
	}

	/**
	 * Parse MySQL ERR packet: 0xFF + error_code (2 bytes) + '#' + sqlstate (5 bytes) + message
	 */
	private function parseErrorMessage(string $packet): string {
		if (strlen($packet) < 3) {
			return 'Unknown error';
		}
		$errorCode = unpack('v', substr($packet, 1, 2))[1];
		$offset = 3;

		// Optional SQL state marker (Protocol 4.1+)
		if (isset($packet[$offset]) && $packet[$offset] === '#') {
			$sqlState = substr($packet, $offset + 1, 5);
			$offset += 6;
		} else {
			$sqlState = '';
		}

		$message = substr($packet, $offset);
		return sprintf('[%d] %s%s', $errorCode, $sqlState ? "({$sqlState}) " : '', $message);
	}

	/**
	 * Naive but safe parameter interpolation.
	 * Replaces ? placeholders with properly escaped values.
	 *
	 * NOTE: For production use, consider COM_STMT_PREPARE (prepared statements)
	 * which keeps escaping server-side and avoids injection entirely.
	 */
	private function interpolate(string $sql, array $params): string {
		if (empty($params)) {
			return $sql;
		}

		$i = 0;
		return preg_replace_callback('/\?/', function () use ($params, &$i): string {
			if (!array_key_exists($i, $params)) {
				throw new \RuntimeException("Parameter index {$i} missing");
			}
			$value = $params[$i++];

			if ($value === null) {
				return 'NULL';
			}
			if (is_bool($value)) {
				return $value ? '1' : '0';
			}
			if (is_int($value) || is_float($value)) {
				return (string) $value;
			}
			// String — escape single quotes
			return "'" . str_replace(
				["\\", "'", "\x00", "\n", "\r", "\x1a"],
				["\\\\", "\\'", "\\0", "\\n", "\\r", "\\Z"],
				(string) $value
			) . "'";
		}, $sql);
	}

	private function assertConnected(): void {
		if ($this->socket === null || !is_resource($this->socket)) {
			throw new \RuntimeException(
				'MySQL connection is not established. Call connect()->await() first.'
			);
		}
	}
}
