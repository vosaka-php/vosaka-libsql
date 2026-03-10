<?php

declare(strict_types=1);

namespace vennv\vosaka\libsql;

interface AsyncConnection {
	public function query(string $sql, array $params = []): QueryResult;
	public function execute(string $sql, array $params = []): ExecuteResult;
	public function close(): void;
}
