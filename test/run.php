<?php
/**
 * 队列运行
 * User: javion
 */
require_once dirname(__DIR__) . '/vendor/autoload.php';
use Javion\cu_test\Queue;

$objQueue = new Queue();
$objQueue->startService();
