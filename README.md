# Parquet to SQL (Laravel)

Helper para importar arquivos Parquet diretamente para tabelas PostgreSQL usando Laravel. Le o arquivo em lotes, tenta usar `COPY` para performance e faz fallback para inserts em massa.

## Instalacao

```bash
composer require dotted/parquet-to-sql
php artisan vendor:publish --tag=config --provider="ParquetToSql\\ParquetToSqlServiceProvider"
```

Dependencias:
- ext-pgsql habilitada no PHP para que o `pgsqlCopyFromArray` funcione.
- `codercat/php-parquet` (ja esta no composer.json) para fazer o parse do arquivo.

## Uso rapido (CLI)

```bash
php artisan parquet:import storage/app/data/users.parquet users \\
  --truncate \\
  --map=id=user_id --map=email=email_address
```

Opcoes principais:
- `--connection=` escolhe a conexao (default: `config('parquet-to-sql.connection')`).
- `--batch=` tamanho do lote (default: `config('parquet-to-sql.batch_size')`).
- `--timeout=` timeout do COPY em segundos.
- `--truncate` roda `TRUNCATE TABLE` antes de inserir.

## Uso programatico

```php
use ParquetToSql\ParquetImporter;

$importer = app(ParquetImporter::class);

$result = $importer->import(
    path: storage_path('app/data/users.parquet'),
    table: 'users',
    columnMap: ['id' => 'user_id', 'email' => 'email_address'],
    truncateBeforeImport: true,
);

logger()->info('Import', $result->toArray());
```

## Enfileirando

```php
use ParquetToSql\ImportParquetJob;

ImportParquetJob::dispatch(
    path: storage_path('app/data/users.parquet'),
    table: 'users',
    columnMap: ['id' => 'user_id'],
    connectionName: 'pgsql',
    truncateBeforeImport: false
);
```

## Configuracao

Arquivo `config/parquet-to-sql.php`:
- `connection`: conexao de banco usada pelo importador.
- `batch_size`: quantas linhas sao lidas antes de enviar para o banco.
- `copy_timeout`: timeout do COPY em segundos.

## Notas e limites

- O codigo tenta usar `PDO::pgsqlCopyFromArray`; se nao existir (ex.: SQLite em testes), cai para inserts em lote.
- E esperado que os nomes de tabela e colunas sejam seguros (apenas letras, numeros, `_` e `.`, mais o esquema opcional).
- O leitor padrao e otimista em relacao as variacoes da lib `codercat/php-parquet`. Se sua versao expuser metodos diferentes, implemente um `ParquetRowReader` proprio e passe no importador.
