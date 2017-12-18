#!/usr/bin/env php
<?php
$key = 'BF@' . date('Ymd', time());
$m = 4294967296;
$k = 8;

$bf = new Bf($key, $m, $k);
$bf->flushall();

$fp = fopen('./sample.txt', 'r');
while ($word = fgets($fp)) {
	$word = trim($word);
	if (empty($word)) {
		continue;
	}
	$rt = $bf->add($word);
	if ($rt) {
		error_log('WARNING: ' . $word . ' EXIST!');
	}
}
fclose($fp);

class Bf
{

	public $redis;
	public $key;
	public $m;
	public $k;

	public function __construct($key, $m, $k) {
		if ($m > 4294967296) {
			error_log('ERROR: m over 4294967296');
			return false;
		}
		$this->key = $key;
		$this->m = $m;
		$this->k = $k;
		$this->redis = new Redis();
		$this->redis->connect('127.0.0.1', 6379);
	}

	public function add($e) {
		$e = (string)$e;
		$this->redis->multi(Redis::PIPELINE);
		for ($i = 0; $i < $this->k; $i ++) {
			$seed = self::getBKDRHashSeed($i);
			$hash = self::BKDRHash($e, $seed);
			$offset = $hash % $this->m;
			$this->redis->setbit($this->key, $offset, 1);
		}
		$t1 = microtime(true);
		$rt = $this->redis->exec();
		$t2 = microtime(true);
		$cost = round(($t2-$t1)*1000, 3).'ms';
		$c = array_sum($rt);
		error_log('[' . date('Y-m-d H:i:s', time()) . '] DEBUG: redis-time-spent=' . $cost . ' entry=' . $e . ' c=' . $c);
		return $c === $this->k;
	}

	public function flushall() {
		return $this->redis->delete($this->key);
	}

	static public function getBKDRHashSeed($n) {
		if ($n === 0) return 31;
		$j = $n + 2;
		$r = 0;
		for ($i = 0; $i < $j; $i ++) {
			if ($i % 2) {// 奇数
				$r = $r * 10 + 3;
			} else {
				$r = $r * 10 + 1;
			}
		}
		return $r;
	}

	static public function BKDRHash($str, $seed) {
		$hash = 0;
		$len = strlen($str);
		$i = 0;
		while ($i < $len) {
			$hash = ((floatval($hash * $seed) & 0x7FFFFFFF) + ord($str[$i])) & 0x7FFFFFFF;
			$i ++;
		}
		return ($hash & 0x7FFFFFFF);
	}
}
