<?php
// $Id$

namespace DynamoUtil;

use Aws\DynamoDb\Enum\Type;
use Aws\DynamoDb\Enum\AttributeAction;
use Aws\DynamoDb\Enum\ReturnValue;
use Aws\DynamoDb\Enum\TableStatus;

class Exception extends \Exception {}

class DynamoUtil
{
    static protected $defaults = null;
    static protected $dynamodb = null;

    static public function _init()
    {
        \Config::load('dynamo_util', true);
        static::$defaults = \Config::get('dynamo_util.defaults');
    }

    static public function getDynamo()
    {
        if (static::$dynamodb === null) {
            $awsconfig = array(
                'key' => \Config::get('dynamo_util.defaults.key'),
                'secret' => \Config::get('dynamo_util.defaults.secret'),
                'region' => \Config::get('dynamo_util.defaults.region'),
                );
            // setup DynamoDB
            $aws = \Aws\Common\Aws::factory($awsconfig);
            $dynamodb = $aws->get('dynamodb');

            static::$dynamodb = $dynamodb;
        }
        return static::$dynamodb;
    }

    static public function addTablePrefix($name)
    {
        $prefix = \Config::get('dynamo_util.defaults.table_prefix');
        if (!empty($prefix)) {
            if (strncmp($name, $prefix, strlen($prefix)) !== 0) {
                $name = $prefix . $name;
            }
        }
        return $name;
    }

    static public function createTable($options = null)
    {
        $table = array_val($options, 'TableName');
        $schema = array_val($options, 'KeySchema');
        $tput = array_val($options, 'ProvisionedThroughput', 1);
        $wait = array_val($options, 'wait', false);

        if (is_int($tput)) {
            $tput = array(
                'ReadCapacityUnits' => $tput,
                'WriteCapacityUnits' => $tput,
                );
        } elseif (!is_array($tput)) {
            throw new Exception('invalid ProvisionedThroughput value');
        }

        $table = static::addTablePrefix($table);

        $params = array(
            'TableName' => $table,
            'KeySchema' => $schema,
            'ProvisionedThroughput' => $tput,
            );

        $dynamodb = self::getDynamo();
        $dynamodb->createTable($params);

        if ($wait) {
            // waiting for activate
            do {
                sleep(1);
                $desc = self::describeTable($table);
            } while($desc['TableStatus'] !== TableStatus::ACTIVE);
        }
    }

    static public function describeTable($table_name)
    {
        $table_name = static::addTablePrefix($table_name);

        $params = array(
            'TableName' => $table_name,
            );
        $dynamodb = self::getDynamo();
        $response = $dynamodb->describeTable($params);
        $desc = $response->getPath('Table');

        return $desc;
    }

    static protected function adjustNumber($val)
    {
        if (strpos($val, '.') === false) {
            $result = (int)$val;
        } else {
            $result = (double)$val;
        }
        return $result;
    }

    static public function typeAdjust($typeval)
    {
        $result = null;
        foreach ($typeval as $type => $val) {
            switch ($type) {
            case Type::STRING:
            case Type::STRING_SET:
                $result = $val;
                break;
            case Type::NUMBER:
                $result = self::adjustNumber($val);
                break;
            case Type::NUMBER_SET:
                $result = array();
                foreach ($val as $v) {
                    $result[] = self::adjustNumber($v);
                }
                break;
            // do not support BINARY and BINARY_SET yet.
            }

            // just one time
            break;
        }
        return $result;
    }
}
