<?php
declare(strict_types=1);
require __DIR__ . "/../vendor/autoload.php";
use vosaka\foroutines\Launch;
use vosaka\foroutines\Thread;
use vosaka\foroutines\AsyncIO;
use vosaka\foroutines\Delay;

Launch::new(function() {
    for ($i = 0; $i < 5; $i++) {
        Delay::new(1000);
        echo "Pending IO: " . (AsyncIO::hasPending() ? "YES (" . AsyncIO::pendingCount() . ")" : "NO") . "\n";
        echo "Active tasks count: " . Launch::$activeCount . "\n";
        echo "Active Queue size: " . Launch::$queue->count() . "\n";
    }
    echo "Terminating test.\n";
    exit(1);
});
Thread::await();
