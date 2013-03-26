<?php

namespace DynamoUtil;

use Aws\DynamoDb\Enum\AttributeAction;
use Aws\DynamoDb\Enum\Type;
use Aws\DynamoDb\Enum\ReturnValue;

class Sequence extends TableBase
{
    const SEQ_ITEM_ID = 1;
    
    protected $keySchema = array(
        'HashKeyElement' => array(
            'AttributeName' => 'id',
            'AttributeType' => Type::NUMBER,
            ),
        );

    public function __construct($seqname)
    {
        if (empty($seqname)) {
            throw new Exception('seqname is required');
        }

        if (preg_match('/^seq_/', $seqname) !== 1) {
            // sequance table must be start with 'seq_'
            $seqname = 'seq_' . $seqname;
        }

        parent::__construct($seqname);
    }

    public function create($options = null)
    {
        $read_cap = array_val($options, 'read', 1);
        $write_cap = array_val($options, 'write', 1);
        $initialize = array_val($options, 'initialize', true);

        $params = array(
            'TableName' => $this->tableName,
            'KeySchema' => $this->keySchema,
            'ProvisionedThroughput' => array(
                'ReadCapacityUnits' => $read_cap,
                'WriteCapacityUnits' => $write_cap,
                ),
            );
        if ($initialize) {
            $params['wait'] = true;
        }

        DynamoUtil::createTable($params);

        if ($initialize) {
            $this->initialize();
        }
    }

    public function initialize($start = 1)
    {
        $data = array(
            'id' => self::SEQ_ITEM_ID,
            'sequence' => (int)$start,
            );
        $this->put($data);
    }

    public function exists()
    {
        try {
            $desc = $this->describe(true);
        }
        catch (\Aws\DynamoDb\Exception\ResourceNotFoundException $e) {
            return false;
        }
        return true;
    }

    public function current()
    {
        $data = $this->scan();
        return array_val($data[0], 'sequence');
    }

    public function next()
    {
        $cur = $this->current();
        $keys = array(
            'id' => self::SEQ_ITEM_ID,
            );
        $vals = array(
            'sequence' => array(
                'Action' => AttributeAction::ADD,
                'Value' => array(
                    Type::NUMBER => '1',
                    ),
                ),
            );
        $options = array(
            'ReturnValues' => ReturnValue::ALL_NEW,
            );
        $result = $this->update($keys, $vals, $options);
        return array_val($result, 'sequence');
    }
}
