<?php

use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Expression;
use ParquetToSql\Contracts\ParquetRowReader;
use ParquetToSql\ParquetImporter;
use PHPUnit\Framework\TestCase;

class ParquetImporterTest extends TestCase
{
    public function testCopyUsesTransactionAndTimeout(): void
    {
        $path = $this->makeTempFile();
        $reader = $this->makeReader(['id', 'name'], [
            ['id' => 1, 'name' => 'alice'],
            ['id' => 2, 'name' => 'bob'],
        ]);

        $pdo = new class {
            public array $calls = [];

            public function pgsqlCopyFromArray($table, $lines, $delimiter, $nullAs, $columns): int
            {
                $this->calls[] = compact('table', 'lines', 'delimiter', 'nullAs', 'columns');

                return count($lines);
            }
        };

        $connection = $this->createMock(ConnectionInterface::class);
        $connection->expects($this->once())->method('getPdo')->willReturn($pdo);
        $connection->expects($this->once())->method('beginTransaction');
        $connection->expects($this->once())->method('statement')->with('SET LOCAL statement_timeout = 5000');
        $connection->expects($this->once())->method('commit');
        $connection->method('rollBack');
        $connection->expects($this->never())->method('table');

        $importer = new ParquetImporter($connection, batchSize: 2, copyTimeoutSeconds: 5);
        $result = $importer->import($path, 'public.users', [], $reader);

        $this->assertSame(2, $result->rowsImported);
        $this->assertCount(1, $pdo->calls);

        $call = $pdo->calls[0];
        $this->assertSame('public.users', $call['table']);
        $this->assertSame(["1\talice", "2\tbob"], $call['lines']);
        $this->assertSame('\\N', $call['nullAs']);
        $this->assertSame('id,name', $call['columns']);
    }

    public function testCopyRollsBackOnError(): void
    {
        $path = $this->makeTempFile();
        $reader = $this->makeReader(['id'], [['id' => 1]]);

        $pdo = new class {
            public function pgsqlCopyFromArray(): void
            {
                throw new RuntimeException('boom');
            }
        };

        $connection = $this->createMock(ConnectionInterface::class);
        $connection->expects($this->once())->method('getPdo')->willReturn($pdo);
        $connection->expects($this->once())->method('beginTransaction');
        $connection->expects($this->once())->method('rollBack');
        $connection->expects($this->once())->method('statement')->with('SET LOCAL statement_timeout = 5000');
        $connection->expects($this->never())->method('commit');

        $importer = new ParquetImporter($connection, batchSize: 1, copyTimeoutSeconds: 5);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('COPY command failed: boom');

        $importer->import($path, 'users', [], $reader);
    }

    public function testFallbackInsertUsesQuotedTableAndNormalization(): void
    {
        $path = $this->makeTempFile();
        $timestamp = new DateTimeImmutable('2023-01-02T03:04:05Z');

        $reader = $this->makeReader(['id', 'meta', 'created_at'], [
            ['id' => 1, 'meta' => ['foo' => 'bar'], 'created_at' => $timestamp],
        ]);

        $pdo = new stdClass();

        $table = new class {
            public array $inserted = [];

            public function insert(array $rows): void
            {
                $this->inserted = $rows;
            }
        };

        $connection = $this->createMock(ConnectionInterface::class);
        $connection->method('getPdo')->willReturn($pdo);
        $connection->expects($this->once())
            ->method('table')
            ->with($this->callback(function ($expression) {
                return $expression instanceof Expression && (string) $expression === '"schema"."users"';
            }))
            ->willReturn($table);
        $connection->expects($this->never())->method('beginTransaction');
        $connection->expects($this->never())->method('statement');

        $importer = new ParquetImporter($connection, batchSize: 10, copyTimeoutSeconds: 5);
        $importer->import($path, 'schema.users', [], $reader);

        $this->assertSame([
            [
                'id' => 1,
                'meta' => '{"foo":"bar"}',
                'created_at' => $timestamp->format('Y-m-d H:i:s.uP'),
            ],
        ], $table->inserted);
    }

    public function testInvalidColumnNameThrows(): void
    {
        $path = $this->makeTempFile();
        $reader = $this->makeReader(['id', 'bad.column'], [
            ['id' => 1, 'bad.column' => 2],
        ]);

        $connection = $this->createMock(ConnectionInterface::class);
        $connection->method('getPdo')->willReturn(new stdClass());

        $importer = new ParquetImporter($connection);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid column name: bad.column');

        $importer->import($path, 'users', [], $reader);
    }

    private function makeTempFile(): string
    {
        $path = tempnam(sys_get_temp_dir(), 'parquet');
        file_put_contents($path, 'test');

        return $path;
    }

    private function makeReader(array $columns, array $rows): ParquetRowReader
    {
        return new class ($columns, $rows) implements ParquetRowReader {
            private array $columns;
            private array $rows;

            public function __construct(array $columns, array $rows)
            {
                $this->columns = $columns;
                $this->rows = $rows;
            }

            public function rows(): iterable
            {
                return $this->rows;
            }

            public function columns(): array
            {
                return $this->columns;
            }
        };
    }
}
