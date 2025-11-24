<?php

namespace ParquetToSql\Contracts;

interface ParquetRowReader
{
    /**
        * Returns an iterable of associative arrays representing rows.
        *
        * @return iterable<int, array<string, mixed>>
        */
    public function rows(): iterable;

    /**
        * Columns available in the Parquet file.
        *
        * @return array<int, string>
        */
    public function columns(): array;
}
