<?php
/**
 * ιεθΏθ‘
 * User: javion
 */
require_once dirname(__DIR__) . '/vendor/autoload.php';
use Javion\cu_test\Queue;

$objQueue = new Queue();
$objQueue->startService();
