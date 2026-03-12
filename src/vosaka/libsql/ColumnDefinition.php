<?php

declare(strict_types=1);

namespace vosaka\libsql;

final readonly class ColumnDefinition {
	public function __construct(
		public string $name,
		public int $typeOid,    // PostgreSQL OID  -or-  MySQL type byte
		public int $tableOid = 0,
		public int $attrNum = 0,
		public int $typeSize = 0,
		public int $typeMod = 0,
		public int $formatCode = 0, // 0 = text, 1 = binary (Postgres only)
		// MySQL extras — zero/empty for Postgres
		public int $flags = 0,
		public int $charset = 0,
		public int $decimals = 0,
	) {
	}
}
