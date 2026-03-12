# Vosaka LibSQL

**Vosaka LibSQL** is an asynchronous database library for PHP, built on top of the `vosaka-foroutines` ecosystem. It provides fully non-blocking, concurrent database operations with built-in connection pooling for both **MySQL** and **PostgreSQL**.

## Features

- **Asynchronous & Non-blocking**: Leverages PHP fibers (via `vosaka-foroutines`) to handle multiple queries concurrently without blocking the main execution thread.
- **Connection Pools**: Built-in efficient connection pooling (`AsyncMysqlPool` and `AsyncPgsqlPool`).
- **Multiple Databases**: First-class support for both MySQL and PostgreSQL.
- **High Performance**: Designed for high-throughput applications requiring vast amounts of concurrent database operations.
- **Type-safe & Modern**: Developed with modern PHP 8 features and strict typing.

## Installation

You can install the library via Composer:

```bash
composer require venndev/vosaka-libsql
```

*(Note: This library requires the `venndev/vosaka-fourotines` package)*

## Usage

### 1. MySQL Connection Pool

Here is a quick example of how to use the `AsyncMysqlPool` to run concurrent queries non-blocking:

```php
<?php

require 'vendor/autoload.php';

use vosaka\libsql\AsyncMysqlPool;
use vosaka\foroutines\AsyncMain;
use vosaka\foroutines\Launch;
use vosaka\foroutines\Thread;
use vosaka\foroutines\RunBlocking;

#[AsyncMain]
function main(): void
{
    // Initialize the MySQL pool
    $pool = new AsyncMysqlPool(
        host: '127.0.0.1',
        port: 3306,
        user: 'root',
        password: 'password',
        database: 'test_db',
        maxConnections: 20
    );

    // Run concurrent queries
    RunBlocking::new(function () use ($pool) {
        for ($i = 0; $i < 50; $i++) {
            Launch::new(function () use ($pool, $i) {
                // Non-blocking query execution
                $result = $pool->query('SELECT * FROM users WHERE id = ?', [$i % 10 + 1])->await();
                
                // For INSERT/UPDATE/DELETE
                $pool->execute('INSERT INTO logs (message) VALUES (?)', ["Log entry $i"])->await();
            });
        }
        Thread::await();
    });

    // Clean up
    $pool->closeAll()->await();
}
```

### 2. PostgreSQL Connection Pool

Using PostgreSQL is just as easy with `AsyncPgsqlPool`:

```php
<?php

require 'vendor/autoload.php';

use vosaka\libsql\AsyncPgsqlPool;
use vosaka\foroutines\AsyncMain;
use vosaka\foroutines\Launch;
use vosaka\foroutines\Thread;
use vosaka\foroutines\RunBlocking;

#[AsyncMain]
function main(): void
{
    // Initialize the PostgreSQL pool
    $pool = new AsyncPgsqlPool(
        host: '127.0.0.1',
        port: 5432,
        user: 'postgres',
        password: 'password',
        database: 'test_db',
        maxConnections: 20
    );

    RunBlocking::new(function () use ($pool) {
        Launch::new(function () use ($pool) {
            $result = $pool->query('SELECT * FROM users LIMIT 10')->await();
        });
        Thread::await();
    });

    $pool->closeAll()->await();
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open-sourced software licensed under the MIT license.
