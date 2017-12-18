#!/usr/bin/env php
<?php
$a = $_SERVER['argv'][1];
$k = $_SERVER['argv'][2];
$n = empty($_SERVER['argv'][3]) ? 0 : intval($_SERVER['argv'][3]);

$fpr = pow(1 - exp(-$k/$a), $k);
echo 'FPR: ' . $fpr . "\n";
if ($n) {
	echo 'FPN: ' . $fpr * $n . "\n";
	echo 'Mem: ' . $a * $n / 8 / 1024 / 1024 . "MB\n";
}

# vim: set ts=4 sw=4 cindent nu :
