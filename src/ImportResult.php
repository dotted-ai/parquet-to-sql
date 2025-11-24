<?php

namespace ParquetToSql;

class ImportResult
{
    public function __construct(
        public readonly string $sourcePath,
        public readonly string $table,
        public readonly int $rowsImported,
        public readonly float $durationSeconds
    ) {
    }

    public function toArray(): array
    {
        return [
            'source_path' => $this->sourcePath,
            'table' => $this->table,
            'rows_imported' => $this->rowsImported,
            'duration_seconds' => $this->durationSeconds,
        ];
    }
}
