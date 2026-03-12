<?php

declare(strict_types=1);

namespace vosaka\libsql;

/**
 * Result of an INSERT / UPDATE / DELETE statement.
 */
final class ExecuteResult
{
    public function __construct(
        public readonly int $affectedRows,
        public readonly int|string|null $lastInsertId,
    ) {}
}
