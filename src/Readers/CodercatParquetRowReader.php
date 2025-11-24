<?php

namespace ParquetToSql\Readers;

use ParquetToSql\Contracts\ParquetRowReader;
use RuntimeException;

class CodercatParquetRowReader implements ParquetRowReader
{
    private string $path;

    public function __construct(string $path)
    {
        $this->path = $path;
    }

    public function rows(): iterable
    {
        $reader = $this->makeReader();

        try {
            yield from $this->rowsFromReader($reader);
        } finally {
            $this->closeIfPossible($reader);
        }
    }

    public function columns(): array
    {
        $reader = $this->makeReader();

        try {
            $columns = $this->columnsFromSchema($reader);

            if (!empty($columns)) {
                return $columns;
            }

            foreach ($this->rowsFromReader($reader) as $row) {
                return array_keys((array) $row);
            }

            return [];
        } finally {
            $this->closeIfPossible($reader);
        }
    }

    private function makeReader(): object
    {
        $readerClass = $this->resolveReaderClass();

        if (method_exists($readerClass, 'fromFile')) {
            return $readerClass::fromFile($this->path);
        }

        $stream = fopen($this->path, 'rb');
        if ($stream === false) {
            throw new RuntimeException("Unable to open Parquet file: {$this->path}");
        }

        if (method_exists($readerClass, 'fromStream')) {
            return $readerClass::fromStream($stream);
        }

        $reader = new $readerClass();

        if (method_exists($reader, 'open')) {
            $reader->open($this->path);
        }

        return $reader;
    }

    private function columnsFromSchema(object $reader): array
    {
        $schema = null;

        if (method_exists($reader, 'getSchema')) {
            $schema = $reader->getSchema();
        } elseif (property_exists($reader, 'schema')) {
            $schema = $reader->schema;
        }

        if ($schema === null) {
            return [];
        }

        $fields = [];
        if (method_exists($schema, 'getColumns')) {
            $fields = $schema->getColumns();
        } elseif (method_exists($schema, 'getFields')) {
            $fields = $schema->getFields();
        } elseif (is_iterable($schema)) {
            $fields = $schema;
        }

        $names = [];
        foreach ($fields as $field) {
            if (is_object($field) && method_exists($field, 'getName')) {
                $names[] = $field->getName();
            } elseif (is_object($field) && property_exists($field, 'name')) {
                $names[] = $field->name;
            } elseif (is_array($field) && isset($field['name'])) {
                $names[] = $field['name'];
            }
        }

        return $names;
    }

    private function rowsFromReader(object $reader): iterable
    {
        if (method_exists($reader, 'read')) {
            foreach ((array) $reader->read() as $row) {
                yield (array) $row;
            }
            return;
        }

        if (method_exists($reader, 'readRows')) {
            foreach ($reader->readRows() as $row) {
                yield (array) $row;
            }
            return;
        }

        $rowGroupCount = $this->rowGroupCount($reader);
        if ($rowGroupCount > 0 && method_exists($reader, 'getRowGroup')) {
            for ($index = 0; $index < $rowGroupCount; $index++) {
                $group = $reader->getRowGroup($index);
                foreach ($this->rowsFromGroup($group) as $row) {
                    yield $row;
                }
            }

            return;
        }

        if (is_iterable($reader)) {
            foreach ($reader as $row) {
                yield (array) $row;
            }

            return;
        }

        throw new RuntimeException('Could not iterate over Parquet rows. Please ensure codercat/php-parquet is installed and up to date.');
    }

    private function rowsFromGroup(object $group): iterable
    {
        if (method_exists($group, 'readRows')) {
            foreach ($group->readRows() as $row) {
                yield (array) $row;
            }
            return;
        }

        if (method_exists($group, 'read')) {
            foreach ($group->read() as $row) {
                yield (array) $row;
            }
            return;
        }

        if (method_exists($group, 'getRows')) {
            foreach ($group->getRows() as $row) {
                yield (array) $row;
            }
            return;
        }

        if (is_iterable($group)) {
            foreach ($group as $row) {
                yield (array) $row;
            }
        }
    }

    private function rowGroupCount(object $reader): int
    {
        if (method_exists($reader, 'getRowGroupCount')) {
            return (int) $reader->getRowGroupCount();
        }

        if (method_exists($reader, 'rowGroupCount')) {
            return (int) $reader->rowGroupCount();
        }

        if (method_exists($reader, 'getRowGroups')) {
            return count($reader->getRowGroups());
        }

        return 0;
    }

    private function closeIfPossible(object $reader): void
    {
        if (method_exists($reader, 'close')) {
            $reader->close();
        }
    }

    private function resolveReaderClass(): string
    {
        $candidates = [
            '\\codercat\\parquet\\ParquetReader',
            '\\Codercat\\Parquet\\ParquetReader',
        ];

        foreach ($candidates as $candidate) {
            if (class_exists($candidate)) {
                return $candidate;
            }
        }

        throw new RuntimeException('codercat/php-parquet is required to use the default row reader.');
    }
}
