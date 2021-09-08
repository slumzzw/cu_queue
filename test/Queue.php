<?php
/**
 * Queue test
 * User: javion
 */
namespace Javion\cu_test;
class Queue extends \Javion\cu_queue\Queue
{
    public function __construct()
    {
        $configPath = dirname(__DIR__) . '/src/Config/config.ini';
        parent::__construct($configPath);
    }
}