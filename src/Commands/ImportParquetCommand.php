<?php

namespace ParquetToSql\Commands;

use Illuminate\Console\Command;
use ParquetToSql\ParquetImporter;

class ImportParquetCommand extends Command
{
    protected $signature = 'parquet:import
                            {path : Caminho do arquivo .parquet}
                            {table : Nome da tabela de destino no Postgres}
                            {--map=* : Mapeamento colunaParquet=colunaTabela}
                            {--connection= : Nome da conexao de banco (default: config parquet-to-sql.connection)}
                            {--batch= : Tamanho do lote para copy/insert}
                            {--timeout= : Timeout do COPY em segundos}
                            {--truncate : Executa TRUNCATE TABLE antes de importar}';

    protected $description = 'Importa um arquivo Parquet para uma tabela PostgreSQL usando COPY.';

    public function handle(): int
    {
        $columnMap = $this->parseColumnMap($this->option('map') ?? []);
        $connectionName = $this->option('connection') ?: config('parquet-to-sql.connection');
        $batchSize = (int) ($this->option('batch') ?: config('parquet-to-sql.batch_size'));
        $timeout = (int) ($this->option('timeout') ?: config('parquet-to-sql.copy_timeout'));

        $importer = new ParquetImporter(
            app('db')->connection($connectionName),
            $batchSize,
            $timeout
        );

        $result = $importer->import(
            $this->argument('path'),
            $this->argument('table'),
            $columnMap,
            null,
            (bool) $this->option('truncate')
        );

        $this->info(sprintf(
            'Importados %d registros em %.2fs para a tabela %s.',
            $result->rowsImported,
            $result->durationSeconds,
            $result->table
        ));

        return self::SUCCESS;
    }

    private function parseColumnMap(array $mapOption): array
    {
        $map = [];
        foreach ($mapOption as $pair) {
            if (!str_contains($pair, '=')) {
                $this->warn("Ignorando mapeamento invalido: {$pair}");
                continue;
            }

            [$source, $target] = explode('=', $pair, 2);
            $map[trim($source)] = trim($target);
        }

        return $map;
    }
}
