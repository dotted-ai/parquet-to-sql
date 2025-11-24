<?php

namespace ParquetToSql;

use Illuminate\Support\ServiceProvider;
use ParquetToSql\Commands\ImportParquetCommand;

class ParquetToSqlServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/parquet-to-sql.php', 'parquet-to-sql');

        $this->app->singleton(ParquetImporter::class, function ($app) {
            return new ParquetImporter(
                $app['db']->connection($app['config']->get('parquet-to-sql.connection')),
                $app['config']->get('parquet-to-sql.batch_size'),
                $app['config']->get('parquet-to-sql.copy_timeout'),
            );
        });
    }

    public function boot(): void
    {
        $this->publishes([
            __DIR__ . '/../config/parquet-to-sql.php' => config_path('parquet-to-sql.php'),
        ], 'config');

        if ($this->app->runningInConsole()) {
            $this->commands([ImportParquetCommand::class]);
        }
    }
}
