<?php

declare(strict_types=1);

require __DIR__ . "/../vendor/autoload.php";

use vosaka\libsql\AsyncPgsqlPool;
use vosaka\foroutines\AsyncMain;

#[AsyncMain]
function main(): void {
	$conn = new AsyncPgsqlPool('127.0.0.1', 5432, 'postgres', 'pokiwar0981', 'shop');

	$result = $conn->query('SELECT * FROM users')->await();
	foreach ($result->rows as $row) {
		var_dump($row);
	}

	$result = $conn->execute(
		'INSERT INTO users (name) VALUES ($1)',
		['Nam']
	)->await();

	echo $result->lastInsertId;

	$conn->closeAll()->awaitAll();
}
