<?php

return [
    // Name of the database connection to use when copying into Postgres.
    'connection' => env('PARQUET_TO_SQL_CONNECTION', env('DB_CONNECTION', 'pgsql')),

    // Number of rows read from Parquet before sending them to the database.
    'batch_size' => env('PARQUET_TO_SQL_BATCH_SIZE', 5_000),

    // Timeout (in seconds) applied to COPY statements.
    'copy_timeout' => env('PARQUET_TO_SQL_COPY_TIMEOUT', 300),
];
