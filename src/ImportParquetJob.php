<?php

namespace ParquetToSql;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class ImportParquetJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(
        public string $path,
        public string $table,
        public array $columnMap = [],
        public ?string $connectionName = null,
        public bool $truncateBeforeImport = false
    ) {
    }

    public function handle(ParquetImporter $importer): void
    {
        if ($this->connectionName) {
            $importer = new ParquetImporter(
                app('db')->connection($this->connectionName),
                config('parquet-to-sql.batch_size'),
                config('parquet-to-sql.copy_timeout')
            );
        }

        $importer->import(
            $this->path,
            $this->table,
            $this->columnMap,
            null,
            $this->truncateBeforeImport
        );
    }
}
