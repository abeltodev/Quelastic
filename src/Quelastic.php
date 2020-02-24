<?php
/**
 * User: abel
 * Date: 27/05/19
 */


namespace Abeltodev\Quelastic;

use Elasticsearch\ClientBuilder;
use Illuminate\Support\Facades\Log;
use Sentry\SentryLaravel\SentryFacade;

class Quelastic
{
    public $params;

    public $query = [];

    public $aggs = [];

    public $sort = [];

    public $client;

    public $withSource = true;

    public $from = 0;

    public $size = 20; // Default size

    protected $results;

    public function __construct()
    {
        $hosts = config('constants.elastic.hosts');

        $this->client = ClientBuilder::create()
            ->setHosts($hosts)
            ->build();
    }

    /*
     * Queries
     */

    public function clear()
    {
        $this->query = [];

        $this->sort = [];

        $this->aggs = [];

        $this->params['body'] = [];

        return $this;
    }

    public function params($params)
    {
        $this->params = $params;

        return $this;
    }

    public function where($key, $value)
    {
        $this->query['filter'][] = ['term' => [$key => $value]];

        return $this;
    }

    public function whereNot($key, $value)
    {
        $this->query['filter'][] = [
            'bool' => [
                'must_not' => ['term' => [$key => $value]]
            ]
        ];

        return $this;
    }

    public function whereIn($key, $value)
    {
        $this->query['filter'][] = ['terms' => [$key => $value]];

        return $this;
    }

    public function whereKeywords($keywords, $boost, $required = false)
    {
        $keywords = is_array($keywords) ? $keywords : [$keywords];
        $required = $required ? 'must' : 'should';

        $this->query[$required][]['multi_match'] = [
            'query' => implode($keywords, ' '),
            'fields' => $boost
        ];

        return $this;
    }

    public function whereRange($key, $operator, $value)
    {
        $this->query['filter'][] = ['range' => [
            $key => [
                $operator => $value
            ]
        ]];

        return $this;
    }

    public function rawFilter($raw)
    {
        $this->query['filter'][] = $raw;

        return $this;
    }

    public function order($field, $direction = 'desc')
    {
        $this->sort[] = [
            $field => [
                'order' => $direction
            ]
        ];

        return $this;
    }

    public function sort($field, $values)
    {
        if (!is_array($values)) {
            $values = [$values];
        }

        foreach ($values as $value) {
            $this->sort[] = [
                '_script' => [
                    'script' => [
                        'lang' => 'painless',
                        'source' => "doc[params.fieldParam].value == params.valueParam ? 1 : 0",
                        'params' => ['fieldParam' => $field, 'valueParam' => $value],
                    ],
                    'type' => 'number',
                    'order' => 'desc'
                ]
            ];
        }
    }

    public function aggregation($name, $field, $size = 50)
    {
        $this->aggs[$name]['terms'] = [
            'field' => $field,
            'size' => $size
        ];

        return $this;
    }

    public function wildcard($key, $value)
    {
        $this->query['filter'][] = ['wildcard' => [$key => "*{$value}*"]];

        return $this;
    }

    public function size($size)
    {
        $this->params['body']['size'] = $size;

        return $this;
    }

    public function limit($from, $size = false)
    {
        $this->from = $from;
        $this->size = ($size !== false && $size !== null) ? $size : $this->size;

        return $this;
    }

    public function exists($key)
    {
        $this->query['filter'][] = [
            'exists' => [
                'field' => $key
            ]
        ];

        return $this;
    }

    public function notExists($key)
    {
        $this->query['filter'][] = [
            'bool' => [
                'must_not' => ['exists' => ['field' => $key]]
            ]
        ];

        return $this;
    }

    /*
     * Calls
     */

    public function execute()
    {
        if (!empty($this->query)) {
            $this->params['body']['query']['bool'] = $this->query;
        }

        if (!empty($this->sort)) {
            $this->params['body']['sort'] = $this->sort;
        }

        if (!empty($this->aggs)) {
            $this->params['body']['aggs'] = $this->aggs;
        }

        if (!$this->withSource) {
            $this->params['_source'] = false;
        }

        $this->params["from"] = $this->from;

        $this->params["size"] = $this->size;

        try {
            return $this->client->search($this->params);
        } catch (\Exception $e) {
            Log::warning('[CATCH] QueryBuilder', ['message' => $e->getMessage()]);

            SentryFacade::captureException($e);

            if (env('APP_DEBUG', false)) {
                dd(json_decode($e->getMessage()));
            }
        }
    }

    public function find($id)
    {
        try {
            return $this->client->get(['id' => $id, 'index' => $this->params['index']]);
        } catch (\Exception $e) {
            Log::warning('[CATCH] QueryBuilder', ['message' => $e->getMessage()]);

            SentryFacade::captureException($e);

            if (env('APP_DEBUG', false)) {
                dd(json_decode($e->getMessage()));
            }
        }
    }

    public function get()
    {
        $results = $this->execute();

        return $this->transform($results);
    }
}
