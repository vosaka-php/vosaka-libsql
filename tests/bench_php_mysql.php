<?php

declare(strict_types=1);

require __DIR__ . "/../vendor/autoload.php";

use vennv\vosaka\libsql\AsyncMysqlPool;
use vosaka\foroutines\AsyncMain;
use vosaka\foroutines\Launch;
use vosaka\foroutines\RunBlocking;

const DB_HOST    = '127.0.0.1';
const DB_PORT    = 3307;
const DB_USER    = 'testuser';
const DB_PASS    = 'testpass';
const DB_NAME    = 'shop';
const POOL_SIZE  = 20;   // intentionally smaller than CONCURRENT to test wait behavior
const CONCURRENT = 50;
const ITERATIONS = 200;

function bench(string $label, callable $fn): void {
	$fn(); // warmup
	$start   = hrtime(true);
	$fn();
	$elapsed = (hrtime(true) - $start) / 1e6;
	$qps     = round(ITERATIONS / ($elapsed / 1000), 1);
	echo str_pad($label, 38) . " | " .
		str_pad(round($elapsed, 2) . " ms", 12) .
		" | {$qps} q/s\n";
}

#[AsyncMain]
function main(): void {
	$pool = new AsyncMysqlPool(
		host: DB_HOST,
		port: DB_PORT,
		user: DB_USER,
		password: DB_PASS,
		database: DB_NAME,
		maxConnections: POOL_SIZE,
	);

	echo "\n";
	echo "PHP vosaka-foroutines+pool — MySQL Benchmark\n";
	echo "Concurrent fibers : " . CONCURRENT . "\n";
	echo "Pool size         : " . POOL_SIZE  . "\n";
	echo "Total iterations  : " . ITERATIONS . "\n";
	echo str_repeat("─", 65) . "\n";
	echo str_pad("Scenario", 38) . " | " .
		str_pad("Time", 12)     . " | Throughput\n";
	echo str_repeat("─", 65) . "\n";

	// ── 1. Sequential SELECT ──────────────────────────────────────────
	bench("Sequential SELECT (×" . ITERATIONS . ")", function () use ($pool) {
		RunBlocking::new(function () use ($pool) {
			for ($i = 1; $i <= ITERATIONS; $i++) {
				$pool->query('SELECT * FROM users WHERE id = ?', [$i % 10 + 1])->await();
			}
		});
	});

	// ── 2. Concurrent SELECT ──────────────────────────────────────────
	bench("Concurrent SELECT (×" . CONCURRENT . " fibers)", function () use ($pool) {
		RunBlocking::new(function () use ($pool) {
			$perFiber = (int) ceil(ITERATIONS / CONCURRENT);
			for ($f = 0; $f < CONCURRENT; $f++) {
				$offset = $f;
				Launch::new(function () use ($pool, $perFiber, $offset) {
					for ($i = 0; $i < $perFiber; $i++) {
						$pool->query(
							'SELECT * FROM users WHERE id = ?',
							[($offset + $i) % 10 + 1]
						)->await();
					}
				});
			}
		});
	});

	// ── 3. Sequential INSERT ──────────────────────────────────────────
	bench("Sequential INSERT (×" . ITERATIONS . ")", function () use ($pool) {
		for ($i = 0; $i < ITERATIONS; $i++) {
			$pool->execute(
				'INSERT INTO users (name) VALUES (?)',
				['BenchUser_' . $i]
			)->await();
		}
	});

	// ── 4. Concurrent INSERT ──────────────────────────────────────────
	bench("Concurrent INSERT (×" . CONCURRENT . " fibers)", function () use ($pool) {
		RunBlocking::new(function () use ($pool) {
			$perFiber = (int) ceil(ITERATIONS / CONCURRENT);
			for ($f = 0; $f < CONCURRENT; $f++) {
				$fid = $f;
				Launch::new(function () use ($pool, $perFiber, $fid) {
					for ($i = 0; $i < $perFiber; $i++) {
						$pool->execute(
							'INSERT INTO users (name) VALUES (?)',
							["Fiber{$fid}_Row{$i}"]
						)->await();
					}
				});
			}
		});
	});

	// ── 5. Mixed R+W ─────────────────────────────────────────────────
	bench("Mixed R+W (×" . CONCURRENT . " fibers)", function () use ($pool) {
		RunBlocking::new(function () use ($pool) {
			$perFiber = (int) ceil(ITERATIONS / CONCURRENT);
			for ($f = 0; $f < CONCURRENT; $f++) {
				$fid = $f;
				Launch::new(function () use ($pool, $perFiber, $fid) {
					for ($i = 0; $i < $perFiber; $i++) {
						if ($i % 2 === 0) {
							$pool->query(
								'SELECT * FROM users WHERE id = ?',
								[$i % 10 + 1]
							)->await();
						} else {
							$pool->execute(
								'INSERT INTO users (name) VALUES (?)',
								["Mixed{$fid}_{$i}"]
							)->await();
						}
					}
				});
			}
		});
	});

	$pool->closeAll()->await();

	echo str_repeat("─", 65) . "\n";
	echo "Done.\n\n";
}
