<?php

declare(strict_types=1);

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;

if ($argc !== 2) {
    fwrite(STDERR, "usage: php generate_avro_compatibility.php path/to/vendor/autoload.php\n");
    exit(2);
}

require $argv[1];

$fixture = json_decode(
    file_get_contents(__DIR__.'/avro_generic_wrapper.json'),
    true,
    512,
    JSON_THROW_ON_ERROR,
);
$schema = AvroSchema::parse($fixture['schema']);
$json = json_encode(
    $fixture['value'],
    JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION,
);
if ($json !== $fixture['json']) {
    throw new RuntimeException('PHP JSON encoding differs from the compatibility fixture.');
}
$io = new AvroStringIO();
$io->write("\x00");
$writer = new AvroIODatumWriter($schema);
$writer->write(
    ['json' => $json, 'version' => 1],
    new AvroIOBinaryEncoder($io),
);
$datum = substr($io->string(), 1);
$reader = new AvroIODatumReader($schema);
$decoded = $reader->read(new AvroIOBinaryDecoder(new AvroStringIO($datum)));
if ($decoded !== ['json' => $json, 'version' => 1]) {
    throw new RuntimeException('PHP Apache Avro did not round-trip the compatibility fixture.');
}

echo base64_encode($io->string()).PHP_EOL;
