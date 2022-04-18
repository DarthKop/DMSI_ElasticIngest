###      TIMER     ###
$startTime = Get-Date
### FORMAT AND FILE ###
Add-Type -AssemblyName System.Web.Extensions
#$dateTimeFormat = New-Object -TypeName System.IFormatProvider -ArgumentList 'MM/dd/yyyyHH:mm:ss'
$Files = Get-ChildItem #Path to file you want to ingest
### RUNSPACE PARTS ###
[Console]::ForegroundColor = 'White'
[Console]::WriteLine("Creating Runspace...")
$Throttle = 20 #threads
$sessionState = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
$RunSpacePool = [RunspaceFactory]::CreateRunspacePool(1, $Throttle, $sessionState, $Host)
$RunSpacePool.Open()
$Global:Workers = [System.Collections.ArrayList]::new()
$ErrorActionPreference = 'Continue'
### MAIN SCRIPT ###
foreach($File in $Files)
    { 
    #This is the script block for the runspaces
    $ScriptBlock = {
        Param ($File)
        $FileName = $File.Name
        [Console]::WriteLine($FileName)
        #Get information for file
        $validTables = @()#valid table names
        $dbtable = ([regex]::match($FileName, '.*app').Groups[0].Value)
        if ($validTables -notcontains $dbtable) {continue}
        #Elastic ingest where data will go
        $index = #Name of index you want data to go into
        $FileName = $FileName -replace "$dbtable`_"
        $Customer = ([regex]::match($FileName, '.*?_').Groups[0].Value) -replace "_"
        $FileName = $FileName -replace "$Customer`_"
        $Env = ([regex]::match($FileName, '.*?_').Groups[0].Value) -replace "_"
        #counter for ingest limit
        $ingestCount = 0
        #create StreamReader to read file and Arraylist to hold record
        $FileStreamReader = [System.IO.StreamReader]::new($File.FullName)
        $FileRecordArray = [System.Collections.ArrayList]::new()
        #While loop to add records into Arraylist
        while (!($FileStreamReader.EndOfStream)) {
            $line = $FileStreamReader.ReadLine()
            #check if line contains soap and rest, skip if it does  
            if (($line.Contains("SOAP Service Name")) -or ($line.Contains("REST Service Name"))) 
            {
                continue
            }else{
                #replace bad chars with good ingestable chars
                $line = $line.replace([char]0x001, ',').replace([char]0x003, '=').replace([char]0x004, '=').replace([char]0x002, ',')
                #add to arrayList
                [void]$FileRecordArray.Add($line)
            }
            #clear line
            $line = $null
        }#END OF WHILE LOOP
        #dispose of StreamReader
        $FileStreamReader.Dispose()
        #initialize tables for each record
        $Global:HT = @{}
        $pair = @{}
        #initialize variables for Elastic bulk ingest
        $URI = #ElasticSearch Bulk ingest URI
        $APIHeader = #Elastic credentials
        $BulkHeader = '{"index" : {"_index" : "' + $index + '"}}'
        #Json Serializer
        $jsonSerializer = [System.Web.Script.Serialization.JavaScriptSerializer]::new()    
        #StringBuilder for Ingesting        
        $IngestOutput = [System.Text.StringBuilder]::new()
        #Foreach record in arraylist
        foreach ($Line in $FileRecordArray)
        {
            #Keeping track of how many records to ingest
            $ingestCount++
            #Splits each record up and replaces bad chars with ingestable chars
            $Line -split ',' | ForEach-Object { $pair = $_ -split '='; if ( -not $HT.ContainsKey($pair[0])) { $HT.Add($pair[0], $pair[1]) } }
            $pair = $null
            #Creates good date format for elastic
            $datetime = [datetime]::parseExact(($HT.'Date' + $HT.'Time'), 'MM/dd/yyyyHH:mm:ss', $null).ToString('o')
            #Add to HT
            $HT.Add('datetime', $datetime)
            $HT.Add('table_name', $dbtable)
            $HT.Add('customer_name', $Customer)
            #build output for ingesting
            [void]$IngestOutput.AppendLine($BulkHeader)
            [void]$IngestOutput.Append($jsonSerializer.Serialize($HT))
            [void]$IngestOutput.Append("`n")
            [void]$HT.Clear()
            #Ingest if IngestOutput has 1000 in it
            if($ingestCount -eq 1000)
            {
                Invoke-RestMethod -Headers $APIHeader -Uri $URI -Method Post -ContentType 'application/x-ndjson' -body $IngestOutput
                $IngestOutput.Length = 0
                [void]$HT.Clear()
                $ingestCount = 0
            }#End If
        }#END FOREACH LINE IN FILERECORDARRAY
        if($ingestCount -gt 0)
        {
            [Console]::WriteLine("Final Ingest for: " + $File.Name)
            Invoke-RestMethod -Headers $APIHeader -Uri $URI -Method Post -ContentType 'application/x-ndjson' -body $IngestOutput
            $IngestOutput.Length = 0
            [void]$HT.Clear()
            $ingestCount = 0
        }
    }#END OF SCRIPTBLOCK
    #Creating the jobs
    $Job = [powershell]::create()
    [void]$Job.AddScript($ScriptBlock)
    [void]$Job.AddParameter("File",$File)
    $Job.RunSpacePool = $RunSpacePool
    [void]$Global:Workers.Add([pscustomobject]@{pipe=$Job; Status=$Job.begininvoke()})
}#END OF FOREACH FILE IN FILES
[Console]::ForegroundColor = 'Yellow'
[Console]::WriteLine("Waiting for Workers...")
Do {Start-Sleep -Seconds 5} while ($Global:Workers.status.iscompleted -contains $false)
[Console]::ForegroundColor = 'Green'
[Console]::WriteLine("All runspace Workers done.") 
[void]$RunSpacePool.Close()
[void]$RunSpacePool.Dispose()
$endTime = Get-Date
$elapsedTime = $endTime - $startTime
[Console]::WriteLine($elapsedTime)
Remove-Variable * -ErrorAction SilentlyContinue; Remove-Module *; $error.Clear()
#Ingest Source Logging
#(Get-Content "\\aspnas00\Alpha\Scripts\elastic\ingest_source.ps1") | % {Invoke-Expression $_}
