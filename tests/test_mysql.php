<?php

declare(strict_types=1);

require __DIR__ . "/../vendor/autoload.php";

use vosaka\libsql\AsyncMysqlConnection;
use vosaka\libsql\AsyncMysqlPool;
use vosaka\foroutines\AsyncMain;

#[AsyncMain]
function main(): void
{
    $conn = new AsyncMysqlPool('127.0.0.1', 3307, 'testuser', 'testpass', 'shop');

    $result = $conn->query('SELECT * FROM users', [1])->await();
    foreach ($result->rows as $row) {
        var_dump($row);
    }

    $result = $conn->execute('INSERT INTO users (name) VALUES (?)', ['Nam'])->await();
    echo $result->lastInsertId;
}
