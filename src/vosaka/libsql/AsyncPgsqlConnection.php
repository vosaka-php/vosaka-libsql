<?php

declare(strict_types=1);

namespace vosaka\libsql;

use RuntimeException;
use vosaka\foroutines\AsyncIO;
use vosaka\foroutines\LazyDeferred;

/**
 * AsyncPgsqlConnection — Non-blocking PostgreSQL client built on vosaka-foroutines AsyncIO.
 *
 * Implements the PostgreSQL Frontend/Backend Protocol (v3.0).
 * @see https://www.postgresql.org/docs/current/protocol.html
 *
 * Usage (inside Launch::new or Async::new):
 *
 *     $conn = new AsyncPgsqlConnection('127.0.0.1', 5432, 'postgres', 'secret', 'mydb');
 *     $conn->connect()->await();
 *
 *     $result = $conn->query('SELECT * FROM users WHERE id = $1', [1])->await();
 *     foreach ($result->rows as $row) { ... }
 *
 *     $result = $conn->execute('INSERT INTO users (name) VALUES ($1)', ['Nam'])->await();
 *     echo $result->lastInsertId;
 *
 *     $conn->close()->await();
 *
 * Auth support:
 *   - AuthenticationOk (no password)
 *   - AuthenticationMD5Password
 *   - AuthenticationSASL (SCRAM-SHA-256 / SCRAM-SHA-256-PLUS — RFC 5802)
 *
 * All public methods return LazyDeferred — call ->await() to execute.
 * Must be called from within a Fiber context (Launch::new / Async::new).
 */
final class AsyncPgsqlConnection implements AsyncDatabaseConnection {
	/** @var resource|null */
	private mixed $socket = null;

	// Auth method codes from server
	private const AUTH_OK                = 0;
	private const AUTH_MD5               = 5;
	private const AUTH_SASL              = 10;
	private const AUTH_SASL_CONTINUE     = 11;
	private const AUTH_SASL_FINAL        = 12;

	// Frontend message type bytes
	private const MSG_STARTUP            = '';   // No type byte — special
	private const MSG_PASSWORD           = 'p';
	private const MSG_QUERY              = 'Q';
	private const MSG_PARSE              = 'P';
	private const MSG_BIND               = 'B';
	private const MSG_DESCRIBE           = 'D';
	private const MSG_EXECUTE            = 'E';
	private const MSG_SYNC               = 'S';
	private const MSG_TERMINATE          = 'X';

	// Backend message type bytes
	private const MSG_AUTH               = 'R';
	private const MSG_READY              = 'Z';
	private const MSG_ROW_DESC           = 'T';
	private const MSG_DATA_ROW           = 'D';
	private const MSG_CMD_COMPLETE       = 'C';
	private const MSG_ERROR              = 'E';
	private const MSG_NOTICE             = 'N';
	private const MSG_PARAM_STATUS       = 'S';
	private const MSG_BACKEND_KEY        = 'K';
	private const MSG_PARSE_COMPLETE     = '1';
	private const MSG_BIND_COMPLETE      = '2';
	private const MSG_NO_DATA            = 'n';
	private const MSG_EMPTY_QUERY        = 'I';

	/** Backend PID + secret key — for cancellation requests */
	private int $backendPid    = 0;
	private int $backendSecret = 0;

	public function __construct(
		private readonly string $host,
		private readonly int $port,
		private readonly string $user,
		private readonly string $password,
		private readonly string $database,
		private readonly float $timeoutSeconds = 10.0,
	) {
	}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

	/**
	 * Connect to PostgreSQL and perform startup/auth.
	 * Must be called before any query/execute.
	 */
	public function connect(): LazyDeferred {
		return new LazyDeferred(function (): void {
			$this->socket = $this->doBlockingTcpConnect();
			$this->doStartup();
		});
	}

	/**
	 * Execute a SELECT-style query and return rows + field metadata.
	 *
	 * Uses the Simple Query protocol (single round-trip).
	 * For parameterised queries, delegates to the Extended Query protocol
	 * (Parse → Bind → Execute → Sync) to support $1 placeholders safely.
	 *
	 * @param string $sql    SQL with $1 … $N placeholders
	 * @param array  $params Values for placeholders
	 * @return LazyDeferred Resolves to QueryResult
	 */
	public function query(string $sql, array $params = []): LazyDeferred {
		return new LazyDeferred(function () use ($sql, $params): QueryResult {
			$this->assertConnected();

			if (empty($params)) {
				return $this->simpleQuery($sql);
			}

			return $this->extendedQuery($sql, $params);
		});
	}

	/**
	 * Execute an INSERT/UPDATE/DELETE statement.
	 *
	 * @param string $sql    SQL with $1 … $N placeholders
	 * @param array  $params Values for placeholders
	 * @return LazyDeferred Resolves to ExecuteResult
	 */
	public function execute(string $sql, array $params = []): LazyDeferred {
		return new LazyDeferred(function () use ($sql, $params): ExecuteResult {
			$this->assertConnected();
			$result = $this->extendedQuery($sql, $params);
			return new ExecuteResult($result->affectedRows, $result->lastInsertId);
		});
	}

	/**
	 * Send a simple query to check connection health.
	 *
	 * @return LazyDeferred Resolves to bool (true = alive)
	 */
	public function ping(): LazyDeferred {
		return new LazyDeferred(function (): bool {
			if ($this->socket === null) {
				return false;
			}
			try {
				$this->simpleQuery(';');
				return true;
			} catch (RuntimeException) {
				return false;
			}
		});
	}

	/**
	 * Close the connection gracefully (Terminate message).
	 */
	public function close(): LazyDeferred {
		return new LazyDeferred(function (): void {
			if ($this->socket === null) {
				return;
			}
			try {
				// Terminate message: 'X' + int32 length (4)
				$this->sendMessage(self::MSG_TERMINATE, '');
			} catch (RuntimeException) {
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

    // -------------------------------------------------------------------------
    // Connection + startup
    // -------------------------------------------------------------------------

	/**
	 * Blocking TCP connect then switch to non-blocking — same rationale as MySQL client.
	 * @see AsyncMysqlConnection::doBlockingTcpConnect()
	 */
	private function doBlockingTcpConnect(): mixed {
		$address = "tcp://{$this->host}:{$this->port}";
		$errno = 0;
		$errstr = '';

		$socket = @stream_socket_client(
			$address,
			$errno,
			$errstr,
			$this->timeoutSeconds,
			STREAM_CLIENT_CONNECT,
		);

		if ($socket === false) {
			throw new RuntimeException(
				"Failed to connect to PostgreSQL at {$address}: [{$errno}] {$errstr}"
			);
		}

		stream_set_blocking($socket, false);

		return $socket;
	}

	/**
	 * Send StartupMessage (protocol v3.0) and process auth exchange.
	 *
	 * StartupMessage has NO type byte — just int32 length then int32 protocol,
	 * followed by null-terminated key=value pairs, then a final null byte.
	 */
	private function doStartup(): void {
		// Build parameter section
		$params = ''
			. "user\x00"     . $this->user     . "\x00"
			. "database\x00" . $this->database . "\x00"
			. "client_encoding\x00UTF8\x00"
			. "\x00"; // terminator

		$protocolVersion = pack('N', 196608); // 3.0 → (3 << 16 | 0)
		$body = $protocolVersion . $params;

		// Length includes itself (4 bytes)
		$length = 4 + strlen($body);
		$this->writeRaw(pack('N', $length) . $body);

		// Process messages until ReadyForQuery
		$this->processAuthExchange();
	}

	/**
	 * Drive the auth state machine until ReadyForQuery ('Z').
	 * Handles: AuthenticationOk, MD5, SASL (SCRAM-SHA-256).
	 */
	private function processAuthExchange(): void {
		while (true) {
			[$type, $payload] = $this->readMessage();

			switch ($type) {
				case self::MSG_AUTH:
					$authCode = unpack('N', substr($payload, 0, 4))[1];
					$this->handleAuth($authCode, substr($payload, 4));
					break;

				case self::MSG_PARAM_STATUS:
					// e.g. server_version, TimeZone — ignore for now
					break;

				case self::MSG_BACKEND_KEY:
					$this->backendPid    = unpack('N', substr($payload, 0, 4))[1];
					$this->backendSecret = unpack('N', substr($payload, 4, 4))[1];
					break;

				case self::MSG_READY:
					return; // transaction status byte in payload — ignored

				case self::MSG_ERROR:
					throw new RuntimeException('PostgreSQL auth error: ' . $this->parseError($payload));

				case self::MSG_NOTICE:
					break; // suppress

				default:
					// Unknown message during startup — skip
					break;
			}
		}
	}

	private function handleAuth(int $code, string $data): void {
		match ($code) {
			self::AUTH_OK            => null, // no-op
			self::AUTH_MD5           => $this->doMd5Auth($data),
			self::AUTH_SASL          => $this->doSaslAuth($data),
			default                  => throw new RuntimeException(
				"Unsupported PostgreSQL auth method: {$code}"
			),
		};
	}

    // -------------------------------------------------------------------------
    // Auth implementations
    // -------------------------------------------------------------------------

	/**
	 * MD5 auth: md5(md5(password + user) + salt)
	 * Server sends 4-byte salt; we reply with PasswordMessage.
	 */
	private function doMd5Auth(string $data): void {
		$salt = substr($data, 0, 4);
		$hash = 'md5' . md5(md5($this->password . $this->user) . $salt);
		$this->sendMessage(self::MSG_PASSWORD, $hash . "\x00");

		[$type, $payload] = $this->readMessage();
		if ($type === self::MSG_ERROR) {
			throw new RuntimeException('PostgreSQL MD5 auth error: ' . $this->parseError($payload));
		}
		// Expect AUTH_OK (R + 0x00000000)
	}

	/**
	 * SASL auth — SCRAM-SHA-256 (RFC 5802).
	 *
	 * Flow:
	 *   Server  →  AuthenticationSASL        (list of mechanisms)
	 *   Client  →  SASLInitialResponse        (mechanism + client-first-message)
	 *   Server  →  AuthenticationSASLContinue (server-first-message)
	 *   Client  →  SASLResponse               (client-final-message)
	 *   Server  →  AuthenticationSASLFinal    (server-final-message — verify)
	 *   Server  →  AuthenticationOk
	 */
	private function doSaslAuth(string $data): void {
		// Parse null-terminated list of offered mechanisms
		$mechanisms = explode("\x00", rtrim($data, "\x00"));

		$mechanism = in_array('SCRAM-SHA-256', $mechanisms, true)
			? 'SCRAM-SHA-256'
			: throw new RuntimeException(
				'No supported SASL mechanism. Server offers: ' . implode(', ', $mechanisms)
			);

		// --- Client-first-message ---
		$clientNonce   = base64_encode(random_bytes(18)); // 24-char base64
		$clientFirstBare = "n=,r={$clientNonce}";        // no channel binding → gs2-header = "n,,"
		$gs2Header     = "n,,";
		$clientFirst   = $gs2Header . $clientFirstBare;

		// SASLInitialResponse: mechanism\0 + int32 length + client-first
		$initResponse = $mechanism . "\x00"
			. pack('N', strlen($clientFirst))
			. $clientFirst;
		$this->sendMessage(self::MSG_PASSWORD, $initResponse);

		// --- Server-first-message ---
		[$type, $payload] = $this->readMessage();
		if ($type === self::MSG_ERROR) {
			throw new RuntimeException('PostgreSQL SASL error: ' . $this->parseError($payload));
		}
		// type must be AUTH (R), code = AUTH_SASL_CONTINUE
		$code = unpack('N', substr($payload, 0, 4))[1];
		if ($code !== self::AUTH_SASL_CONTINUE) {
			throw new RuntimeException("Expected SASLContinue ({self::AUTH_SASL_CONTINUE}), got {$code}");
		}

		$serverFirst = substr($payload, 4);
		$serverParams = $this->parseSaslAttributes($serverFirst);

		$serverNonce = $serverParams['r'] ?? throw new RuntimeException('SCRAM: missing server nonce');
		$salt        = base64_decode($serverParams['s'] ?? throw new RuntimeException('SCRAM: missing salt'));
		$iterations  = (int) ($serverParams['i'] ?? throw new RuntimeException('SCRAM: missing iterations'));

		if (!str_starts_with($serverNonce, $clientNonce)) {
			throw new RuntimeException('SCRAM: server nonce does not start with client nonce');
		}

		// --- SCRAM key derivation (RFC 5802 §3) ---
		$saltedPassword = $this->scramDeriveKey($this->password, $salt, $iterations);
		$clientKey      = hash_hmac('sha256', 'Client Key', $saltedPassword, true);
		$storedKey      = hash('sha256', $clientKey, true);
		$serverKey      = hash_hmac('sha256', 'Server Key', $saltedPassword, true);

		$channelBinding  = base64_encode($gs2Header); // no TLS channel binding
		$clientFinalNoCB = "c={$channelBinding},r={$serverNonce}";

		$authMessage = "{$clientFirstBare},{$serverFirst},{$clientFinalNoCB}";

		$clientSignature = hash_hmac('sha256', $authMessage, $storedKey, true);
		$clientProof     = $clientKey ^ $clientSignature;
		$serverSignature = hash_hmac('sha256', $authMessage, $serverKey, true);

		$clientFinal = "{$clientFinalNoCB},p=" . base64_encode($clientProof);
		$this->sendMessage(self::MSG_PASSWORD, $clientFinal);

		// --- Server-final-message ---
		[$type, $payload] = $this->readMessage();
		if ($type === self::MSG_ERROR) {
			throw new RuntimeException('PostgreSQL SASL final error: ' . $this->parseError($payload));
		}

		$code = unpack('N', substr($payload, 0, 4))[1];
		if ($code !== self::AUTH_SASL_FINAL) {
			throw new RuntimeException("Expected SASLFinal ({self::AUTH_SASL_FINAL}), got {$code}");
		}

		$serverFinal = substr($payload, 4);
		$finalParams = $this->parseSaslAttributes($serverFinal);

		// Verify server signature — prevents MITM
		$expectedServerSig = base64_encode($serverSignature);
		if (!hash_equals($expectedServerSig, $finalParams['v'] ?? '')) {
			throw new RuntimeException('SCRAM: server signature verification failed');
		}

		// Expect AuthenticationOk next
		[$type, $payload] = $this->readMessage();
		$code = unpack('N', substr($payload, 0, 4))[1];
		if ($type !== self::MSG_AUTH || $code !== self::AUTH_OK) {
			throw new RuntimeException('PostgreSQL SASL: expected AuthenticationOk after SASLFinal');
		}
	}

	/**
	 * SCRAM SaltedPassword = Hi(Normalize(password), salt, i)
	 * Hi(str, salt, i) = U1 XOR U2 XOR … XOR Ui
	 *   where U1 = HMAC(str, salt + INT(1))
	 *         Uk = HMAC(str, U_{k-1})
	 */
	private function scramDeriveKey(string $password, string $salt, int $iterations): string {
		$u = hash_hmac('sha256', $salt . "\x00\x00\x00\x01", $password, true);
		$result = $u;

		for ($i = 1; $i < $iterations; $i++) {
			$u = hash_hmac('sha256', $u, $password, true);
			$result ^= $u;
		}

		return $result;
	}

	/** Parse SCRAM attribute string: "a=val,b=val,..." → ['a' => 'val', ...] */
	private function parseSaslAttributes(string $msg): array {
		$attrs = [];
		foreach (explode(',', $msg) as $part) {
			$eq = strpos($part, '=');
			if ($eq !== false) {
				$attrs[substr($part, 0, $eq)] = substr($part, $eq + 1);
			}
		}
		return $attrs;
	}

    // -------------------------------------------------------------------------
    // Query protocols
    // -------------------------------------------------------------------------

	/**
	 * Simple Query protocol — single message, no params.
	 * Server responds with: RowDescription? DataRow* CommandComplete | EmptyQueryResponse | ErrorResponse
	 * Followed by ReadyForQuery.
	 */
	private function simpleQuery(string $sql): QueryResult {
		$this->sendMessage(self::MSG_QUERY, $sql . "\x00");

		$fields = [];
		$rows   = [];
		$affectedRows  = 0;
		$lastInsertId  = null;

		while (true) {
			[$type, $payload] = $this->readMessage();

			switch ($type) {
				case self::MSG_ROW_DESC:
					$fields = $this->parseRowDescription($payload);
					break;

				case self::MSG_DATA_ROW:
					$rows[] = $this->parseDataRow($payload, $fields);
					break;

				case self::MSG_CMD_COMPLETE:
					[$affectedRows, $lastInsertId] = $this->parseCommandComplete($payload);
					break;

				case self::MSG_EMPTY_QUERY:
					break;

				case self::MSG_READY:
					return new QueryResult($rows, $fields, $affectedRows, $lastInsertId);

				case self::MSG_ERROR:
					// Drain until ReadyForQuery before throwing
					$this->drainUntilReady();
					throw new RuntimeException('PostgreSQL query error: ' . $this->parseError($payload));

				case self::MSG_NOTICE:
					break;

				default:
					break;
			}
		}
	}

	/**
	 * Extended Query protocol — supports $1…$N params, server-side type binding.
	 *
	 * Parse (P) → Bind (B) → Describe (D) → Execute (E) → Sync (S)
	 * All sent in a single write (pipeline), then responses read back.
	 */
	private function extendedQuery(string $sql, array $params): QueryResult {
		// Build all frontend messages in one buffer (pipeline)
		$pipeline = '';

		// Parse: 'P' + statement_name\0 + query\0 + int16 param_count + OIDs (0 = untyped)
		$parseMsgBody = "\x00"               // unnamed statement
			. $sql . "\x00"
			. pack('n', 0);                  // 0 type OIDs — let server infer
		$pipeline .= $this->buildMessage(self::MSG_PARSE, $parseMsgBody);

		// Bind: 'B' + portal\0 + statement\0 + param format codes + param values + result format codes
		$bindMsgBody = "\x00"                // unnamed portal
			. "\x00"                         // unnamed statement
			. pack('n', 0)                   // 0 param format codes → all text
			. pack('n', count($params));     // number of params

		foreach ($params as $param) {
			if ($param === null) {
				$bindMsgBody .= pack('N', 0xFFFFFFFF); // -1 = NULL
			} else {
				$encoded = (string) $param;
				$bindMsgBody .= pack('N', strlen($encoded)) . $encoded;
			}
		}
		$bindMsgBody .= pack('n', 0); // 0 result format codes → all text
		$pipeline .= $this->buildMessage(self::MSG_BIND, $bindMsgBody);

		// Describe portal: 'D' + 'P' + portal\0
		$pipeline .= $this->buildMessage(self::MSG_DESCRIBE, 'P' . "\x00");

		// Execute: 'E' + portal\0 + int32 max_rows (0 = unlimited)
		$pipeline .= $this->buildMessage(self::MSG_EXECUTE, "\x00" . pack('N', 0));

		// Sync: 'S' (closes implicit transaction, triggers ReadyForQuery)
		$pipeline .= $this->buildMessage(self::MSG_SYNC, '');

		$this->writeRaw($pipeline);

		// --- Read responses ---
		$fields        = [];
		$rows          = [];
		$affectedRows  = 0;
		$lastInsertId  = null;

		while (true) {
			[$type, $payload] = $this->readMessage();

			switch ($type) {
				case self::MSG_PARSE_COMPLETE:
				case self::MSG_BIND_COMPLETE:
					break;

				case self::MSG_NO_DATA:
					break;

				case self::MSG_ROW_DESC:
					$fields = $this->parseRowDescription($payload);
					break;

				case self::MSG_DATA_ROW:
					$rows[] = $this->parseDataRow($payload, $fields);
					break;

				case self::MSG_CMD_COMPLETE:
					[$affectedRows, $lastInsertId] = $this->parseCommandComplete($payload);
					break;

				case self::MSG_EMPTY_QUERY:
					break;

				case self::MSG_READY:
					return new QueryResult($rows, $fields, $affectedRows, $lastInsertId);

				case self::MSG_ERROR:
					$this->drainUntilReady();
					throw new RuntimeException('PostgreSQL execute error: ' . $this->parseError($payload));

				case self::MSG_NOTICE:
					break;

				default:
					break;
			}
		}
	}

    // -------------------------------------------------------------------------
    // Message I/O
    // -------------------------------------------------------------------------

	/**
	 * Build a framed frontend message (type byte + int32 length + body).
	 * Length includes itself (4 bytes) but NOT the type byte.
	 */
	private function buildMessage(string $type, string $body): string {
		$length = 4 + strlen($body);
		return $type . pack('N', $length) . $body;
	}

	/**
	 * Send a single frontend message.
	 */
	private function sendMessage(string $type, string $body): void {
		$this->writeRaw($this->buildMessage($type, $body));
	}

	/**
	 * Read one backend message.
	 * Format: char type + int32 length (includes itself) + body.
	 *
	 * @return array{0: string, 1: string} [type, payload]
	 */
	private function readMessage(): array {
		// 5 bytes: 1 type + 4 length
		$header  = $this->readExact(5);
		$type    = $header[0];
		$length  = unpack('N', substr($header, 1, 4))[1];

		// body length = total length - 4 (the length field itself)
		$bodyLen = $length - 4;
		$payload = $bodyLen > 0 ? $this->readExact($bodyLen) : '';

		return [$type, $payload];
	}

	/**
	 * Write raw bytes to the socket via AsyncIO — Fiber suspends until flushed.
	 */
	private function writeRaw(string $data): void {
		AsyncIO::streamWrite($this->socket, $data)->await();
	}

	/**
	 * Read exactly $n bytes cooperatively (same pattern as MySQL client).
	 */
	private function readExact(int $n): string {
		$buffer    = '';
		$remaining = $n;
		$emptyStreak    = 0;
		$maxEmptyStreaks = 1000;

		while ($remaining > 0) {
			if (!is_resource($this->socket) || feof($this->socket)) {
				throw new RuntimeException(
					"PostgreSQL connection closed unexpectedly (expected {$n} bytes, got " . strlen($buffer) . ')'
				);
			}

			$chunk = AsyncIO::streamRead($this->socket, $remaining)->await();

			if ($chunk === '') {
				$emptyStreak++;
				if ($emptyStreak > $maxEmptyStreaks) {
					throw new RuntimeException(
						"PostgreSQL read timed out (expected {$n} bytes, got " . strlen($buffer) . ')'
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
			$buffer   .= $chunk;
			$remaining -= strlen($chunk);
		}

		return $buffer;
	}

	/**
	 * Drain messages until ReadyForQuery — used after an error to resync.
	 */
	private function drainUntilReady(): void {
		while (true) {
			[$type] = $this->readMessage();
			if ($type === self::MSG_READY) {
				return;
			}
		}
	}

    // -------------------------------------------------------------------------
    // Result parsing
    // -------------------------------------------------------------------------

	/**
	 * Parse RowDescription message.
	 * int16 field_count + (name\0 + tableOid + attrNum + typeOid + typeSize + typeMod + format) * N
	 *
	 * @return ColumnDefinition[]
	 */
	private function parseRowDescription(string $payload): array {
		$offset      = 0;
		$fieldCount  = unpack('n', substr($payload, $offset, 2))[1];
		$offset += 2;

		$fields = [];
		for ($i = 0; $i < $fieldCount; $i++) {
			$nullPos = strpos($payload, "\x00", $offset);
			$name    = substr($payload, $offset, $nullPos - $offset);
			$offset  = $nullPos + 1;

			$tableOid    = unpack('N', substr($payload, $offset, 4))[1];
			$offset += 4;
			$attrNum     = unpack('n', substr($payload, $offset, 2))[1];
			$offset += 2;
			$typeOid     = unpack('N', substr($payload, $offset, 4))[1];
			$offset += 4;
			$typeSize    = unpack('n', substr($payload, $offset, 2))[1];
			$offset += 2;
			$typeMod     = unpack('N', substr($payload, $offset, 4))[1];
			$offset += 4;
			$formatCode  = unpack('n', substr($payload, $offset, 2))[1];
			$offset += 2;

			$fields[] = new ColumnDefinition(
				name: $name,
				typeOid: $typeOid,
				tableOid: $tableOid,
				attrNum: $attrNum,
				typeSize: $typeSize,
				typeMod: $typeMod,
				formatCode: $formatCode,
			);
		}

		return $fields;
	}

	/**
	 * Parse DataRow message.
	 * int16 col_count + (int32 length + data | int32 -1 for NULL) * N
	 *
	 * @param ColumnDefinition[] $fields
	 * @return array<string, string|null>
	 */
	private function parseDataRow(string $payload, array $fields): array {
		$offset   = 0;
		$colCount = unpack('n', substr($payload, $offset, 2))[1];
		$offset  += 2;

		$row = [];
		for ($i = 0; $i < $colCount; $i++) {
			$len = unpack('N', substr($payload, $offset, 4))[1];
			$offset += 4;

			$fieldName = $fields[$i]->name ?? (string) $i;

			if ($len === 0xFFFFFFFF) { // -1 as unsigned = NULL
				$row[$fieldName] = null;
			} else {
				$row[$fieldName] = substr($payload, $offset, $len);
				$offset += $len;
			}
		}

		return $row;
	}

	/**
	 * Parse CommandComplete tag.
	 * Examples: "INSERT 0 1", "UPDATE 3", "SELECT 5", "DELETE 2"
	 *
	 * @return array{0: int, 1: int|null} [affectedRows, lastInsertId]
	 */
	private function parseCommandComplete(string $payload): array {
		$tag = rtrim($payload, "\x00");
		$parts = explode(' ', $tag);

		$command = strtoupper($parts[0] ?? '');

		return match ($command) {
			'INSERT' => [
				(int) ($parts[2] ?? 0),           // affected rows
				null,                              // Postgres doesn't return OID in modern versions
			],
			'UPDATE', 'DELETE', 'MOVE', 'FETCH', 'COPY' => [
				(int) ($parts[1] ?? 0),
				null,
			],
			'SELECT' => [
				(int) ($parts[1] ?? 0),            // row count
				null,
			],
			default => [0, null],
		};
	}

	/**
	 * Parse ErrorResponse fields.
	 * Format: (char code + string\0)* then \0 terminator.
	 * Field codes: S=severity, M=message, C=sqlstate, D=detail, H=hint, ...
	 */
	private function parseError(string $payload): string {
		$offset   = 0;
		$fields   = [];
		$len      = strlen($payload);

		while ($offset < $len) {
			$code = $payload[$offset++];
			if ($code === "\x00") {
				break;
			}
			$nullPos       = strpos($payload, "\x00", $offset);
			$value         = substr($payload, $offset, $nullPos - $offset);
			$offset        = $nullPos + 1;
			$fields[$code] = $value;
		}

		$severity = $fields['S'] ?? 'ERROR';
		$sqlState = $fields['C'] ?? '';
		$message  = $fields['M'] ?? 'Unknown error';
		$detail   = isset($fields['D']) ? ' Detail: ' . $fields['D'] : '';
		$hint     = isset($fields['H']) ? ' Hint: '   . $fields['H'] : '';

		return "[{$severity}] ({$sqlState}) {$message}{$detail}{$hint}";
	}

	private function assertConnected(): void {
		if ($this->socket === null || !is_resource($this->socket)) {
			throw new RuntimeException(
				'PostgreSQL connection is not established. Call connect()->await() first.'
			);
		}
	}
}
