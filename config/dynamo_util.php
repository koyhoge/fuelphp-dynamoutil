<?php
// DynamoUtil package config file

return array(
    'defaults' => array(
        // developer key
        'key' => 'PLEASE SET DEVELOPER KEY HERE',

        // developer secret key
        'secret' => 'PLEASE SET SECRET KEY HERE',
        
        // region where used
        'region' => \Aws\Common\Enum\Region::TOKYO,

        // table prefix
        'table_prefix' => '',
        ),

    // Default setup group
    'default_setup' => 'default',

    // Setup groups
    'setups' => array(
        'default' => array(),
        ),
    );

