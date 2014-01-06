$ErrorActionPreference = "Stop"
$WarningPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$base = gwmi -n root\wmi -cl CitrixXenStoreBase
$sid = $base.AddSession("a")
$s = gwmi -n root\wmi -q "select * from CitrixXenStoreSession where sessionid=$($sid.SessionId)"
$s.GetValue("name").value
