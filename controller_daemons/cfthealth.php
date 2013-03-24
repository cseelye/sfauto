<html>
<?php

$unresponsive_threshold = 20; // 20 sec
$dead_threshold = 60 * 60; // 1 hour
$unresponsive_time = time() - $unresponsive_threshold;
$dead_time = time() - $dead_threshold;

// Connect
$db = new mysqli("localhost", "root", "password", "monitor");

// Clusters that are not healthy
$sql = "SELECT name FROM clusters WHERE ishealthy=0";
$res = $db->query($sql);
$unhealthy_clusters = "";
echo "<!-- unhealthy_clusters = " . $unhealthy_clusters . " -->\n";
while ($row = $res->fetch_assoc())
{
	$unhealthy_clusters = $unhealthy_clusters . "," . $row['name'];
	echo "<!-- unhealthy_clusters = " . $unhealthy_clusters . " -->\n";
}
$unhealthy_clusters = ltrim($unhealthy_clusters, ",");
echo "<!-- unhealthy_clusters = " . $unhealthy_clusters . " -->\n";

// How many responsive clients has vdbench failed on
$sql = "SELECT COUNT(*) FROM clients WHERE vdbench_count<=0 AND (vdbench_last_exit != 0 AND vdbench_last_exit != -1) AND timestamp > '$unresponsive_time'";
$res = $db->query($sql);
$row = $res->fetch_assoc();
$fail_count = $row['COUNT(*)'];

// How many resppnsive clients has vdbench stopped on
//$sql = "SELECT COUNT(*) FROM clients WHERE vdbench_count<=0 AND vdbench_last_exit = 0 AND timestamp > '$unresponsive_time'";
//$res = $db->query($sql);
//$row = $res->fetch_assoc();
//$stop_count = $row['COUNT(*)'];

// How many clients are not responding
$sql = "SELECT COUNT(*) FROM clients WHERE timestamp <= '$unresponsive_time' AND timestamp > '$dead_time'";
$res = $db->query($sql);
$row = $res->fetch_assoc();
$unresponsive_count = $row['COUNT(*)'];
$sql = "SELECT COUNT(*) FROM clients WHERE timestamp <= '$dead_time'";
$res = $db->query($sql);
$row = $res->fetch_assoc();
$dead_count = $row['COUNT(*)'];

// Get a list of groups
$sql = "SELECT DISTINCT `group` FROM clients ORDER BY `group` ASC";
$res = $db->query($sql);
if (!$res)
{
	echo $db->error . "\n";
	exit;
}
$groups = array();
while ($row = $res->fetch_assoc())
{
	array_push($groups, $row["group"]);
}

?>
<head>
<?php
// Set the page title according to the client/cluster status
echo "  <title>";
if ($unhealthy_clusters == "")
{
	echo "{Healthy}";
}
else
{
	echo "{UNHEALTHY}";
}
if ($fail_count > 0)
{
	echo "[FAIL]";
}
elseif ($stop_count > 0)
{
	echo "[STOP]";
}
if ($dead_count > 0)
{
	echo "[DEAD]";
}
elseif ($unresponsive_count > 0)
{
	echo "[SICK]";
}
if ($fail_count <= 0 && $stop_count <= 0 && $unresponsive_count <= 0 && $dead_count <= 0)
{
	echo "[HEALTHY]";
}
echo " CFT Health Monitor";
echo "</title>\n";
?>
  <style type="text/css">
    body { background-color: #E4E7EA; font-family: "Helvetica Neue", Helvetica, Arial, sans-serif }
    table { border-collapse: collapse; border-style: none; border-width: 0px }
    h1 { text-align: center }
    .center { text-align: center }
    .client-table { margin: 10px; float: left; }
    .client-table th, .client-table td { text-align: right; padding: 0px 2px }
    .cluster-table { margin: 10px; float: left; text-align: left; }
    .group-row { text-align: center; font-size: 110%; font-weight: bold; border-style: solid; border-width: 3px 0px; border-color: black; }
    .status-row { text-align: left; padding: 1px 0px }
    .left { text-align: left }
    .red { background-color: red; color: yellow }
    .yellow { background-color: yellow }
    .green { background-color: green }
    .stopped { background-color: #C9FFF7 }
    .evenrow { background-color: #EEFFEE }
    .oddrow { background-color: #D1FFD1 }
    .unresponsive { background-color: yellow; font-style: italic }
    .dead { background-color: red; color: yellow; font-style: italic }
  </style>
</head>
<body>
<?php
// Heading showing overall status
if ($unhealthy_clusters != "")
{
	echo "<h1 class='red'>Cluster " . $unhealthy_clusters . " is unhealthy</h1>\n";
}
else
{
	echo "<h1 class='green'>All clusters are healthy</h1>\n";
}

if ($fail_count > 0)
{
	echo "<h1 class='red'>vdbench failed on " . $fail_count  . " clients</h1>\n";
}
if ($dead_count > 0)
{
	echo "<h1 class='red'>" . $dead_count  . " clients have not responded for a long time</h1>\n";
}
if ($unresponsive_count > 0)
{
	echo "<h1 class='yellow'>" . $unresponsive_count  . " clients are not responding</h1>\n";
}
if ($fail_count <= 0 && $unresponsive_count <= 0 && $dead_count <= 0)
{
	echo "<h1 class='green'>All clients are healthy</h1>\n";
}

// Cluster status
$sql = "SELECT name,mvip,ishealthy,current_faults,message,timestamp FROM clusters ORDER BY name ASC";
$res = $db->query($sql);
while($row = $res->fetch_assoc())
{
	echo "<table style='margin: 10px; float: left;'>\n";
	echo "  <tr>";
	echo "<td style='text-align: center; font-size: 110%; font-weight: bold; padding: 0px 20px; border-style: solid; border-width: 3px 0px; border-color: black;' colspan='100' class='center'>Cluster " . $row['name'] . " (" . $row['mvip'] . ")</td>";
	echo "</tr>";
	echo "\n";
	echo "<tr><td style='line-height: 5px' colspan='2'>&nbsp;</td></tr>\n";

// 	if ($row['ishealthy'] == 0)
// 	{
// 		echo "  <tr>";
// 		echo "<td style='background-color: red; color: yellow' colspan='2'> Cluster is not healthy</td>";
// 		echo "</tr>";
// 		echo "\n";
// 	}
// 	else
	if ($row['ishealthy'] != 0)
	{
		echo "  <tr>";
		echo "<td style='background-color: green; font-weight: bold' colspan='2'> Cluster is healthy</td>";
		echo "</tr>";
		echo "\n";
	}

//	if ($row['current_faults'] != null && $row['current_faults'] != "")
//	{
//		echo "  <tr>";
//		//echo "<td>Current cluster faults:</td>";
//		echo "<td style='background-color: red; color: yellow'>" . $row['current_faults'] . "</td>";
//		echo "</tr>";
//		echo "\n";
//	}

	if ($row['message'] != null && $row['message'] != "")
	{
		$message = $row['message'];
		$message = str_replace("\n", "</br>", $message);
		echo "<tr>";
		echo "<td style='background-color: red; color: yellow' colspan='2'>" . $message . "</td>";
		echo "</tr>";
		echo "\n";
	}

	echo "<tr>";
	echo "<td colspan='2'>Last update: " . sprintf("%.1f", microtime(true) - $row['timestamp']) . " sec ago</td>";
	echo "</tr>";
	echo "\n";

	echo "</table>\n";
}

echo "<div style='clear: both'></div>\n";

// Detailed status for all clients
foreach ($groups as $group_name)
{
	$sql = "SELECT mac, hostname, ip, cpu_usage, mem_usage, vdbench_count, vdbench_last_exit, timestamp FROM clients WHERE `group`='$group_name' ORDER BY hostname ASC";
	$res = $db->query($sql);
	$client_count = $res->num_rows;
	$clients = array();
	$client_status = array();
	$client_status["healthy"] = 0;
	$client_status["stopped"] = 0;
	$client_status["failed"] = 0;
	$client_status["unresponsive"] = 0;
	$client_status["dead"] = 0;
	while($row = $res->fetch_assoc())
	{
		$clients[$row['mac']]['mac'] = $row['mac'];
		$clients[$row['mac']]['hostname'] = $row['hostname'];
		$clients[$row['mac']]['ip'] = $row['ip'];
		$clients[$row['mac']]['cpu_usage'] = $row['cpu_usage'];
		$clients[$row['mac']]['mem_usage'] = $row['mem_usage'];
		$clients[$row['mac']]['vdbench_count'] = $row['vdbench_count'];
		$clients[$row['mac']]['vdbench_last_exit'] = $row['vdbench_last_exit'];
		$clients[$row['mac']]['timestamp'] = $row['timestamp'];

		if (microtime(true) -$row['timestamp'] > $dead_threshold)
		{
			$clients[$row['mac']]['status'] = "dead";
			$client_status["dead"]++;
		}
		elseif (microtime(true) -$row['timestamp'] > $unresponsive_threshold)
		{
			$clients[$row['mac']]['status'] = "unresponsive";
			$client_status["unresponsive"]++;
		}
		elseif ($row['vdbench_count'] <= 0)
		{
			if ($row['vdbench_last_exit'] == 0 || $row['vdbench_last_exit'] == -1)
			{
				$clients[$row['mac']]['status'] = "stopped";
				$client_status["stopped"]++;
			}
			else
			{
				$clients[$row['mac']]['status'] = "failed";
				$client_status["failed"]++;
			}
		}
		else
		{
			$clients[$row['mac']]['status'] = "healthy";
			$client_status["healthy"]++;
		}
	}

	echo "<table style='margin: 10px; float: left;'>\n";
	echo "  <tr>";
	if ($group_name == null || $group_name == "")
		echo "<td style='text-align: center; font-size: 110%; font-weight: bold; border-style: solid; border-width: 3px 0px; border-color: black;' colspan='100'>Ungrouped clients (" . $client_count . ")</td>";
	else
		echo "<td style='text-align: center; font-size: 110%; font-weight: bold; border-style: solid; border-width: 3px 0px; border-color: black;' colspan='100'>" . $group_name . " clients (" . $client_count . ")</td>";
	echo "</tr>";
	echo "\n";

	echo "  <tr>";
	echo "<td></td>";
	echo "<td style='text-align: left; padding: 1px 0px'>";
	echo "Healthy: " . $client_status["healthy"];
	echo "</td>";
	echo "<td></td>";
	echo "<td style='text-align: left; padding: 1px 0px' colspan='2'>";
	echo "vdbench stopped: " . $client_status["stopped"];
	echo "</td>";
	echo "<td></td>";
	echo "</tr>";
	echo "\n";

	echo "  <tr>";
	echo "<td></td>";
	echo "<td style='text-align: left; padding: 1px 0px'>";
	echo "Dead: " . $client_status["dead"];
	echo "</td>";
	echo "<td></td>";
	echo "<td style='text-align: left; padding: 1px 0px' colspan='2'>";
	echo "vdbench failed: " . $client_status["failed"];
	echo "</td>";
	echo "<td></td>";
	echo "</tr>";
	echo "\n";

	echo "  <tr>";
	echo "<td></td>";
	echo "<td style='text-align: left; padding: 1px 0px'>";
	echo "Unresponsive: " . $client_status["unresponsive"];
	echo "</td>";
	echo "<td></td>";
	echo "<td></td>";
	echo "<td></td>";
	echo "</tr>";
	echo "\n";

	echo "  <tr>";
	echo "<th width='180' style='text-align: left'>Hostname</th>";
	echo "<th width='140'>IP Address</th>";
	echo "<th width='80'>CPU Usage</th>";
	echo "<th width='80'>MEM usage</th>";
	echo "<th width='80'>vdbench running</th>";
	echo "<th width='120'>Last update</th>";
	echo "<th></th>";
	echo "</tr>";
	echo "\n";

	$rowcount = 0;
	foreach ($clients as $client_mac => $client)
	{
		$rowcount++;
		if ($client["status"] == "dead")
		{
			echo "  <tr class='dead'>";
		}
		elseif ($client["status"] == "unresponsive")
		{
			echo "  <tr class='unresponsive'>";
		}
		elseif ($client["status"] == "failed")
		{
			echo "  <tr class='red'>";
		}
		elseif ($client["status"] == "stopped")
		{
			echo "  <tr class='stopped'>";
		}
		else
		{
			if ($rowcount % 2 == 0)
			{
				echo "  <tr class='evenrow'>";
			}
			else
			{
				echo "  <tr class='oddrow'>";
			}
		}

		echo "<td class='left'>" . $client['hostname'] . "</td><td>" . $client['ip'] . "</td>";
		echo "<td>" . $client['cpu_usage'] . "%</td>";
		echo "<td>" . $client['mem_usage'] . "%</td>";
		if ($client['vdbench_count'] > 0)
		{
			echo "<td>Yes</td>";
		}
		else
		{
			echo "<td>No (" . $client['vdbench_last_exit'] . ")</td>";
		}

		echo "<td>" . sprintf("%.1f", microtime(true) - $client['timestamp']) . " sec ago</td>";
		//echo "<td><a  href=\"?delete=" . $row['hostname'] . "\">Remove</a></td>";
		echo "</tr>";
		echo "\n";
	}
	echo "</table>\n";
}
?>
</body>
</html>
