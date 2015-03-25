<?php
namespace Bankiru\Yii\Logging\Logstash;

use Bankiru\Yii\Logging\Logstash\Formatter\LumberjackFormatter;
use Ekho\Logstash\Lumberjack;

/**
 * выхлоп логов в ZeroMQ
 * например для того, чтобы класть в logStash
 *
 * @package bankiru\logging
 */
class LumberjackRoute extends \CLogRoute
{
    const DEFAULT_WINDOW_SIZE = 5000;

    private static $defaultFormatterOptions = [
        'applicationName' => '',
        'systemName'      => null,
        'extraPrefix'     => null,
        'contextPrefix'   => 'ctxt_',
        'version'         => LumberjackFormatter::V0,
    ];

    /** @var Lumberjack\Client */
    private $client;
    /** @var LumberjackFormatter */
    private $formatter;

    public $host;
    public $port;
    public $cafile;
    public $options = [];

    public function init()
    {
        $windowSize = self::DEFAULT_WINDOW_SIZE;

        if (array_key_exists('window_size', $this->options)) {
            $windowSize = $this->options['window_size'] ?: self::DEFAULT_WINDOW_SIZE;
            unset($this->options['window_size']);
        }

        $formatterOptions = [];
        if (array_key_exists('formatter', $this->options)) {
            if (is_array($this->options['formatter'])) {
                $formatterOptions = [];
            }
            unset($this->options['formatter']);
        }
        $this->initFormatter($formatterOptions);

        try {
            $this->client = new Lumberjack\Client(
                new Lumberjack\SecureSocket(
                    $this->host,
                    $this->port,
                    ['ssl_cafile' => $this->cafile] + $this->options
                ),
                new Lumberjack\Encoder(),
                $windowSize
            );
        } catch (Lumberjack\Exception $ex) {
            error_log((string)$ex);
            error_log("Disabling " . get_class($this));
            $this->enabled = false;
        }
    }

    private function initFormatter(array $formatterOptions)
    {
        $formatterOptions += self::$defaultFormatterOptions;

        $this->formatter = new LumberjackFormatter(
            $formatterOptions['applicationName'],
            $formatterOptions['systemName'],
            $formatterOptions['extraPrefix'],
            $formatterOptions['contextPrefix'],
            $formatterOptions['version']
        );
    }

    /**
     * @param array $logs
     */
    public function processLogs($logs)
    {
        if (!$this->enabled) {
            return;
        }

        try {
            foreach ($this->formatLogs($logs) as $record) {
                $this->client->write($record);
            }
        } catch (Lumberjack\Exception $ex) {
            error_log((string)$ex);
            array_map('error_log', $logs);
            $this->enabled = false;
        }
    }

    /**
     * Formats a log message given different fields.
     * @param array[] $logs
     * @return \Generator
     */
    protected function formatLogs(array $logs)
    {
        foreach($logs as $log) {
            yield $this->formatter->format([
                'level'     => $log[0],
                'category'  => $log[1],
                'timestamp' => $log[2],
                'message'   => $log[3],
            ]);
        }
    }
}