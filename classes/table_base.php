<?php

namespace DynamoUtil;

use Aws\DynamoDb\Enum\AttributeAction;
use Aws\DynamoDb\Enum\ComparisonOperator;
use Aws\DynamoDb\Enum\ReturnValue;
use Aws\DynamoDb\Model\Attribute;
use Aws\DynamoDb\Exception\ValidationException;

class TableBase
{
    protected $dynamodb = null;

    protected $tableName = null;
    protected $keySchema = null;
    protected $attributeDefinitions = null;

    protected $description = null;

    // simple format to define keys
    protected $keyAttrs = null;

    public function __construct($name = null)
    {
        $this->dynamodb = DynamoUtil::getDynamo();
        if (empty($this->tableName) && $name !== null) {
            $this->tableName = $name;
        }

        if (!empty($this->keyAttrs)) {
            $this->setupSchemaByAttr();
        }

        if (empty($this->keySchema)) {
            $this->setupSchema();
        }
    }

    public function getRealTableName() {
        return DynamoUtil::addTablePrefix($this->tableName);
    }

    public function create($tput = 1)
    {
        $opts = array(
            'TableName' => $this->tableName,
            'KeySchema' => $this->keySchema,
            'AttributeDefinitions' => $this->attributeDefinitions,
            'ProvisionedThroughput' => $tput,
            'wait' => true,
            );
        DynamoUtil::createTable($opts);
    }

    public function setupSchema()
    {
        $this->describe(true);
        $this->keySchema = $this->description['KeySchema'];
        $this->attributeDefinitions = $this->description['AttributeDefinitions'];
    }

    public function setupSchemaByAttr()
    {
        $attrs = $this->keyAttrs;
        if (empty($attrs)) {
            // do nothing
            return;
        }

        $keySchema = $attrDefs = array();
        foreach ($attrs as $attr) {
            $keyType = $attr['KeyType'];
            $attributeName = $attr['AttributeName'];
            $attributeType = $attr['AttributeType'];

            $attrDefs[] = array(
                'AttributeName' => $attributeName,
                'AttributeType' => $attributeType,
                );

            $keySchema[] = array(
                'AttributeName' => $attributeName,
                'KeyType' => $keyType,
                );
        }

        $this->keySchema = $keySchema;
        $this->attributeDefinitions = $attrDefs;
    }

    public function describe($update = false)
    {
        if ($update) {
            $this->description = DynamoUtil::describeTable($this->tableName);
        }
        return $this->description;
    }

    protected function convEmptyVal(&$vals)
    {
        foreach ($vals as $key => &$val) {
            // empty values are not allowed except for zero and false,
            // then convert ot false here
            if ($val === '') {
                $val = false;
            }
        }
        return $vals;
    }

    public function put($vals)
    {
        if (empty($vals) || !is_array($vals)) {
            return;
        }
        $vals = $this->convEmptyVal($vals);

        $put = array(
            'TableName' => $this->getRealTableName(),
            'Item' => $this->dynamodb->formatAttributes($vals),
            );
        $result = $this->dynamodb->putItem($put);
    }

    public function get($keys, $options = null)
    {
        $params = array(
            'TableName' => $this->getRealTableName(),
            'Key' => $this->getKeyParams($keys),
            );

        // add options
        $keys = array('AttributesToGet', 'ConsistentRead');
        foreach ($keys as $key) {
            $opt = array_val($options, $key);
            if (!empty($opt)) {
                $params[$key] = $opt;
            }
        }

        $response = $this->dynamodb->getItem($params);
        return $this->pickResponseVals($response, 'get');
    }

    public function update($keys, $vals, $options = null)
    {
        $expected = array_val($options, 'Expected');
        $retvals = array_val($options, 'ReturnValues');

        $vals = $this->convEmptyVal($vals);
        $params = array(
            'TableName' => $this->getRealTableName(),
            'Key' => $this->getKeyParams($keys),
            );

        // build attributes
        $attrs = array();
        foreach ($vals as $name => $val) {
            if ($val === null) {
                // if $val is null then delte
                $item = array('Action' => AttributeAction::DELETE);
            } elseif (!is_array($val)) {
                 $item = $this->dynamodb->formatValue($val,
                                                      Attribute::FORMAT_UPDATE);
            } else {
                // use raw block
                $item = $val;
            }
            $attrs[$name] = $item;
        }
        $params['AttributeUpdates'] = $attrs;

        // 'Expected' support
        if (!empty($expected)) {
            $exp = array();
            foreach ($expected as $name => $val) {
                $exp[$name] = $this->dynamodb->attribute($val, 'expected');
            }
            $params['Expected'] = $exp;
        }

        // 'ReturnValues' support
        if (!empty($retvals)) {
            $valid_retvals = ReturnValue::values();
            if (in_array($retvals, $valid_retvals)) {
                $params['ReturnValues'] = $retvals;
            }
        }

        $response = $this->dynamodb->updateItem($params);

        if (isset($params['ReturnValues'])) {
            return $this->pickResponseVals($response, 'update');
        }
    }

    public function scan($conds = null, $options = null)
    {
        $attrs = array_val($options, 'AttributesToGet');

        $params = array(
            'TableName' => $this->getRealTableName(),
            );

        $filter = array();
        if (!empty($conds)) {
            foreach ($conds as $name => $cond) {
                $filter[$name] = $this->genCondParams($cond);
            }
            $params['ScanFilter'] = $filter;
        }

        if (!empty($attrs)) {
            $params['AttributesToGet'] = $attrs;
        }
        $response = $this->dynamodb->scan($params);
        return $this->pickResponseVals($response, 'scan');
    }

    public function query($keys, $options = null)
    {
        $params = array(
            'TableName' => $this->getRealTableName(),
            );

        $conds = $this->getQueryKeyParams($keys);
        $params = array_merge($params, $conds);

        $response = $this->dynamodb->query($params);
        return $this->pickResponseVals($response, 'query');
    }

    static public function updateTable($options) {
        $read_cap = array_val($options, 'read');
        $write_cap = array_val($options, 'write');

        $params = array(
            'TableName' => $this->getRealTableName(),
            'ProvisionedThroughput' => array(
                'ReadCapacityUnits' => $read_cap,
                'WriteCapacityUnits' => $write_cap,
                ),
            );

        $this->dynamodb->updateTable($params);
    }

    protected function genCondParams($cond)
    {
        if (is_array($cond)) {
            list($op, $val) = $cond;
        } else {
            $op = '=';
            $val = $cond;
        }

        $operator = $this->compareOperator($op);
        $result = array(
            'ComparisonOperator' => $operator,
            'AttributeValueList' => array(
                $this->dynamodb->formatValue($val),
                ),
            );
        return $result;
    }

    protected function compareOperator($op)
    {
        static $sdk_ops = array(
            ComparisonOperator::EQ => array('=', '=='),
            ComparisonOperator::NE => array('!=', '<>'),
            ComparisonOperator::LT => '<',
            ComparisonOperator::LE => '<=',
            ComparisonOperator::GT => '>',
            ComparisonOperator::GE => '>=',
            ComparisonOperator::NULL => null,
            ComparisonOperator::NOT_NULL => null,
            ComparisonOperator::CONTAINS => null,
            ComparisonOperator::NOT_CONTAINS => null,
            ComparisonOperator::IN => null,
            ComparisonOperator::BETWEEN => null,
            ComparisonOperator::BEGINS_WITH => null,
            );
        $ops = array_keys($sdk_ops);
        if (in_array($op, $ops)) {
            // $op is already AWS CONSTANT
            return $op;
        }

        $result = null;
        foreach ($sdk_ops as $aws_op => $val) {
            if ($val === null) {
                continue;
            } elseif (!is_array($val)) {
                $val = array($val);
            }
            if (in_array($op, $val)) {
                $result = $aws_op;
                break;
            }
        }
        return $result;
    }

    protected function pickResponseVals($response, $type = 'get')
    {
        switch ($type) {
        case 'get':
            $elem = 'Item';
            break;
        case 'update':
            $elem = 'Attributes';
            break;
        case 'scan':
        case 'query':
            $elem = 'Items';
            break;
        default:
            return;
        }
        $data = $response->getPath($elem);
        $count = $response->getPath('Count');

        if ($count === null) {
            // single data
            $result = $this->pickVars($data);
        } else {
            // multi data
            $result = array();
            foreach ($data as $datum) {
                $result[] = $this->pickVars($datum);
            }
        }

        return $result;
    }

    protected function pickVars($data)
    {
        $result = array();
        foreach ($data as $name => $val) {
            $result[$name] = DynamoUtil::typeAdjust($val);
        }
        return $result;
    }

    protected function getQueryKeyParams($keys)
    {
        $conds = array();
        foreach ($keys as $name => $val) {
            $conds[$name] = $this->genCondParams($val);
        }
        return array('KeyConditions' => $conds);
    }

    protected function getKeyParams($keys, $format = null)
    {
        $attrs = array();
        foreach ($keys as $name => $val) {
            $attrtype = $this->getAttributeType($name);
            $attrs[$name] = array($attrtype => (string)$val);
        }
        return $attrs;
    }

    protected function getKeyType($name)
    {
        if (empty($this->keySchema)) {
            throw new Exception('Empty keySchema');
        }

        foreach ($this->keySchema as $params) {
            if ($name === $params['AttributeName']) {
                return $params['KeyType'];
            }
        }
        throw new Exception('Key name not found: ' . $name);
    }

    protected function getAttributeType($name)
    {
        if (empty($this->attributeDefinitions)) {
            throw new Exception('Empty attributeDefinitions');
        }

        foreach ($this->attributeDefinitions as $params) {
            if ($name === $params['AttributeName']) {
                return $params['AttributeType'];
            }
        }
        throw new Exception('Key name not found: ' . $name);
    }
}
