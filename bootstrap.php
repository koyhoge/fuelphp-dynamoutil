<?php
/**
 * DyanmoUtil
 *
 * Util classes for AWS DynamoDB I/O library
 *
 * @package    Fuel
 * @author     koyhoge
 * @license    PHP ver 3.0 License
 */

Autoloader::add_namespace('DynamoUtil', __DIR__.'/classes/');

Autoloader::add_classes(
    array(
        'DynamoUtil\\DynamoUtil' => __DIR__.'/classes/dynamo_util.php',
        'DynamoUtil\\Exception' => __DIR__.'/classes/dynamo_util.php',
        'DynamoUtil\\TableBase' => __DIR__.'/classes/table_base.php',
        'DynamoUtil\\Sequence' => __DIR__.'/classes/sequence.php',
        ));

// array_val() util funciton
if (!function_exists('array_val')) {
    function array_val(&$data, $key, $default = null) {
        if (!is_array($data) && !($data instanceof ArrayAccess)) {
            return $default;
        }
        return isset($data[$key])? $data[$key]: $default;
    }
}
