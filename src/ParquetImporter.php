<?php

namespace ParquetToSql;

use Illuminate\Database\ConnectionInterface;
use InvalidArgumentException;
use ParquetToSql\Contracts\ParquetRowReader;
use ParquetToSql\Readers\CodercatParquetRowReader;
use RuntimeException;

class ParquetImporter
{
    private ConnectionInterface $connection;

    private int $batchSize;

    private int $copyTimeoutSeconds;

    public function __construct(ConnectionInterface $connection, int $batchSize = 5_000, int $copyTimeoutSeconds = 300)
    {
        $this->connection = $connection;
        $this->batchSize = max(1, $batchSize);
        $this->copyTimeoutSeconds = max(1, $copyTimeoutSeconds);
    }

    public function import(
        string $path,
        string $table,
        array $columnMap = [],
        ?ParquetRowReader $reader = null,
        bool $truncateBeforeImport = false
    ): ImportResult {
        if (!is_file($path)) {
            throw new InvalidArgumentException("File not found: {$path}");
        }

        $this->assertSafeIdentifier($table, 'table');
        $reader ??= new CodercatParquetRowReader($path);

        $sourceColumns = $reader->columns();
        if (empty($sourceColumns)) {
            throw new RuntimeException('Unable to read columns from Parquet file.');
        }

        $targetColumns = $this->targetColumns($sourceColumns, $columnMap);
        $this->assertSafeIdentifiers($targetColumns, 'column');

        if ($truncateBeforeImport) {
            $this->connection->statement('TRUNCATE TABLE ' . $this->quoteIdentifier($table));
        }

        $start = microtime(true);
        $inserted = 0;
        $batch = [];

        foreach ($reader->rows() as $row) {
            $batch[] = $this->projectRow($row, $sourceColumns, $columnMap);

            if (count($batch) >= $this->batchSize) {
                $this->flushBatch($table, $targetColumns, $batch);
                $inserted += count($batch);
                $batch = [];
            }
        }

        if (!empty($batch)) {
            $this->flushBatch($table, $targetColumns, $batch);
            $inserted += count($batch);
        }

        $duration = microtime(true) - $start;

        return new ImportResult($path, $table, $inserted, $duration);
    }

    private function projectRow(array $row, array $sourceColumns, array $columnMap): array
    {
        $projected = [];
        foreach ($sourceColumns as $sourceColumn) {
            $targetColumn = $columnMap[$sourceColumn] ?? $sourceColumn;
            $value = $row[$sourceColumn] ?? null;
            $projected[$targetColumn] = $this->normalizeValue($value);
        }

        return $projected;
    }

    private function flushBatch(string $table, array $columns, array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $pdo = $this->connection->getPdo();

        if (method_exists($pdo, 'pgsqlCopyFromArray')) {
            $timeoutMs = $this->copyTimeoutSeconds * 1_000;
            $this->connection->statement('SET LOCAL statement_timeout = ' . (int) $timeoutMs);
            $lines = $this->buildCopyLines($rows, $columns);

            $copied = $pdo->pgsqlCopyFromArray($table, $lines, "\t", '\\N', implode(',', $columns));

            if ($copied === false) {
                throw new RuntimeException('COPY command failed.');
            }

            return;
        }

        // Fallback for drivers that do not expose pgsqlCopyFromArray (tests or sqlite).
        $this->connection->table($table)->insert($rows);
    }

    private function targetColumns(array $sourceColumns, array $columnMap): array
    {
        return array_map(
            fn (string $source): string => $columnMap[$source] ?? $source,
            $sourceColumns
        );
    }

    private function buildCopyLines(array $rows, array $columns): array
    {
        $lines = [];

        foreach ($rows as $row) {
            $lineValues = [];
            foreach ($columns as $column) {
                $lineValues[] = $this->formatCopyValue($row[$column] ?? null);
            }
            $lines[] = implode("\t", $lineValues);
        }

        return $lines;
    }

    private function formatCopyValue(mixed $value): string
    {
        if ($value === null) {
            return '\\N';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s.uP');
        }

        if (is_array($value) || is_object($value)) {
            $value = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }

        $string = (string) $value;
        $string = str_replace("\\", "\\\\", $string);
        $string = str_replace("\t", "\\t", $string);
        $string = str_replace(["\r", "\n"], ['\\r', '\\n'], $string);

        return $string;
    }

    private function normalizeValue(mixed $value): mixed
    {
        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s.uP');
        }

        if (is_array($value) || is_object($value)) {
            return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        }

        if (is_resource($value)) {
            return null;
        }

        return $value;
    }

    private function assertSafeIdentifiers(array $identifiers, string $type): void
    {
        foreach ($identifiers as $identifier) {
            $this->assertSafeIdentifier($identifier, $type);
        }
    }

    private function assertSafeIdentifier(string $identifier, string $type): void
    {
        if (!preg_match('/^[A-Za-z0-9_\\.]+$/', $identifier)) {
            throw new InvalidArgumentException("Invalid {$type} name: {$identifier}");
        }
    }

    private function quoteIdentifier(string $identifier): string
    {
        if (str_contains($identifier, '.')) {
            [$schema, $table] = explode('.', $identifier, 2);
            return sprintf('"%s"."%s"', str_replace('"', '""', $schema), str_replace('"', '""', $table));
        }

        return '"' . str_replace('"', '""', $identifier) . '"';
    }
}
