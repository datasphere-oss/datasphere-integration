
Write-Host "Downloading yajss-stable-12.09 ...."
wget http://sourceforge.mirrorservice.org/y/ya/yajsw/yajsw/yajsw-stable-12.09/yajsw-stable-12.09.zip -OutFile yajsw.zip
Write-Host "Downloaded yajss-stable-12.09"
Expand-Archive yajsw.zip -DestinationPath .\
Rename-Item yajsw-stable-12.09 yajsw_server
Write-Host "Exapanded archive and created folders: yajsw_server"


$invocation = (Get-Variable MyInvocation).Value
$directorypath = Split-Path $invocation.MyCommand.Path
$striimdir = (get-item $directorypath ).parent.parent.FullName
Write-Host "Striim home directory:" $striimdir
$striimval = $striimdir -replace "\\", "\\\\"
cd yajsw_server\conf
cat ..\..\wrapper.conf.server | % { $_ -replace "<working-dir>", "$striimval"}  | Set-Content wrapper.conf
Write-Host "Server wrapper.conf replaced in yajsw_server/conf"
cd ..\bat\
cmd.exe /c "installService.bat"
cd ..\..\

if($args[0] -ne "-noDerby") {
    Copy-Item .\yajsw_server\ yajsw_derby -Recurse
    cd yajsw_derby\conf\
    cat ..\..\wrapper.conf.derby | % { $_ -replace "<working-dir>", "$striimval"}  | % { $_ -replace "<striim-dir>", "$striimdir"}  | Set-Content wrapper.conf
    Write-Host "Derby wrapper.conf replaced in yajsw_derby/conf"
    cd ..\bat\
    cmd.exe /c "installService.bat"
    cd ..\..\

    cmd.exe /c "sc config com.datasphere.runtime.Server depend=Derby"
    write-host "Striim is now setup to depend on Derby service"
}




