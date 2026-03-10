<?php
// debug_connect.php
$s = @stream_socket_client('tcp://127.0.0.1:3307', $errno, $errstr, 5, STREAM_CLIENT_CONNECT);
if ($s === false) {
    echo "FAILED: [$errno] $errstr\n";
} else {
    echo "TCP connect OK\n";
    stream_set_blocking($s, false);

    // Đọc server greeting thủ công
    sleep(1); // chờ greeting arrive
    $data = fread($s, 1024);
    echo "Greeting length: " . strlen($data) . " bytes\n";
    echo "First byte: " . (strlen($data) > 0 ? ord($data[0]) : 'EMPTY') . "\n";
    echo "Raw hex: " . bin2hex(substr($data, 0, 20)) . "\n";
    fclose($s);
}
