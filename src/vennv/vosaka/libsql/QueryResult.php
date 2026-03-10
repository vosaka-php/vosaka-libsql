<?php

declare(strict_types=1);

namespace vennv\vosaka\libsql;

/**
 * Result of a SELECT query.
 *
 * @property array<int, array<string, string|null>> $rows
 * @property ColumnDefinition[] $fields
 */
final class QueryResult {
	/**
	 * @param array<int, array<string, string|null>> $rows
	 * @param ColumnDefinition[] $fields
	 */
	public function __construct(
		public readonly array $rows,
		public readonly array $fields,
		public int $affectedRows  = 0,
		public ?int $lastInsertId  = null,
	) {
	}

	public function count(): int {
		return count($this->rows);
	}

	public function isEmpty(): bool {
		return empty($this->rows);
	}

	/**
	 * Return the first row, or null if no rows.
	 * @return array<string, string|null>|null
	 */
	public function first(): ?array {
		return $this->rows[0] ?? null;
	}
}
