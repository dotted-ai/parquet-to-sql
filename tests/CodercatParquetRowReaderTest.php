<?php

use ParquetToSql\Readers\CodercatParquetRowReader;
use PHPUnit\Framework\TestCase;

class CodercatParquetRowReaderTest extends TestCase
{
    public function testReadRowsIsPreferredOverRead(): void
    {
        $reader = new CodercatParquetRowReader(__FILE__);
        $stub = new class {
            public array $called = [];

            public function readRows(): iterable
            {
                $this->called[] = 'readRows';
                yield ['a' => 1];
            }

            public function read(): iterable
            {
                $this->called[] = 'read';
                return [['a' => 2]];
            }
        };

        $rows = $this->invokeRowsFromReader($reader, $stub);

        $this->assertSame([['a' => 1]], $rows);
        $this->assertSame(['readRows'], $stub->called);
    }

    public function testRowGroupIsIteratedWhenAvailable(): void
    {
        $reader = new CodercatParquetRowReader(__FILE__);

        $group = new class {
            public array $called = [];

            public function readRows(): iterable
            {
                $this->called[] = 'readRows';
                yield ['g' => 1];
            }
        };

        $stub = new class ($group) {
            public array $called = [];
            private object $group;

            public function __construct(object $group)
            {
                $this->group = $group;
            }

            public function getRowGroupCount(): int
            {
                $this->called[] = 'getRowGroupCount';
                return 1;
            }

            public function getRowGroup(int $index): object
            {
                $this->called[] = 'getRowGroup';
                return $this->group;
            }
        };

        $rows = $this->invokeRowsFromReader($reader, $stub);

        $this->assertSame([['g' => 1]], $rows);
        $this->assertSame(['getRowGroupCount', 'getRowGroup'], $stub->called);
        $this->assertSame(['readRows'], $group->called);
    }

    public function testIterablesAreReturnedAsRows(): void
    {
        $reader = new CodercatParquetRowReader(__FILE__);
        $iterable = new ArrayIterator([
            ['x' => 1],
            ['x' => 2],
        ]);

        $rows = $this->invokeRowsFromReader($reader, $iterable);

        $this->assertSame([
            ['x' => 1],
            ['x' => 2],
        ], $rows);
    }

    public function testReadIsUsedAsLastResort(): void
    {
        $reader = new CodercatParquetRowReader(__FILE__);
        $stub = new class {
            public array $called = [];

            public function read(): iterable
            {
                $this->called[] = 'read';
                return [['y' => 3]];
            }
        };

        $rows = $this->invokeRowsFromReader($reader, $stub);

        $this->assertSame([['y' => 3]], $rows);
        $this->assertSame(['read'], $stub->called);
    }

    private function invokeRowsFromReader(CodercatParquetRowReader $reader, object $input): array
    {
        $ref = new ReflectionClass($reader);
        $method = $ref->getMethod('rowsFromReader');
        $method->setAccessible(true);

        return iterator_to_array($method->invoke($reader, $input));
    }
}
