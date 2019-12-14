package pl.msulima.redis.benchmark.log.protocol;

enum DecoderState {
    INITIAL, ARRAY_START, BULK_STRING_START, BULK_STRING_READ_RESPONSE, SIMPLE_STRING
}
