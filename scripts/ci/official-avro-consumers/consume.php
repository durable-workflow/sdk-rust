#!/usr/bin/env php
<?php

declare(strict_types=1);

use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use Composer\InstalledVersions;

if ($argc !== 4) {
    fwrite(STDERR, "usage: consume.php <autoload> <payload> <schema>\n");
    exit(2);
}

require $argv[1];

$expectedVersion = '1.12.2.0';
$version = InstalledVersions::getVersion('apache/avro');
if ($version !== $expectedVersion) {
    throw new RuntimeException("expected official PHP apache/avro {$expectedVersion}, got {$version}");
}

$frame = file_get_contents($argv[2]);
$schemaJson = file_get_contents($argv[3]);
if ($frame === false || $schemaJson === false) {
    throw new RuntimeException('could not read the Rust payload or packaged schema');
}
if (substr($frame, 0, 10) !== hex2bin('c301e2a33dff55802237')) {
    throw new RuntimeException('Rust payload has the wrong single-object header');
}

$schema = AvroSchema::parse($schemaJson);
$stream = new AvroStringIO(substr($frame, 10));
$errorReporting = error_reporting();
error_reporting($errorReporting & ~E_WARNING & ~E_DEPRECATED);
try {
    $datum = (new AvroIODatumReader($schema))->read(new AvroIOBinaryDecoder($stream));
} finally {
    error_reporting($errorReporting);
}
if (!$stream->isEof()) {
    throw new RuntimeException('official PHP consumer left trailing datum bytes');
}

$entries = $datum['value']['entries'];
if ($entries['empty_array']['value']['items'] !== []) {
    throw new RuntimeException('official PHP consumer lost the nested empty array');
}
if ($entries['empty_map']['value']['entries'] !== []) {
    throw new RuntimeException('official PHP consumer lost the nested empty map');
}

$nested = $entries['nested']['value']['items'];
$boundaries = $nested[0]['value']['entries'];
if ($boundaries['minimum']['value']['long'] !== PHP_INT_MIN) {
    throw new RuntimeException('official PHP consumer lost i64::MIN');
}
if ($boundaries['maximum']['value']['long'] !== PHP_INT_MAX) {
    throw new RuntimeException('official PHP consumer lost i64::MAX');
}
$scalarArray = $nested[1]['value']['items'];
if ($scalarArray[0]['value']['bytes'] !== "\x00\xff") {
    throw new RuntimeException('official PHP consumer lost non-UTF-8 bytes');
}
if (bin2hex(pack('E', $scalarArray[1]['value']['double'])) !== '8000000000000000') {
    throw new RuntimeException('official PHP consumer lost negative-zero bits');
}

fwrite(STDOUT, "official PHP apache/avro {$version} decoded Rust payload\n");
