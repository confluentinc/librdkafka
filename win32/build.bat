@echo off

SET TOOLCHAIN=v140

nuget restore librdkafka.sln

FOR %%C IN (Debug,Release) DO (
  FOR %%P IN (Win32,x64) DO (
     @echo Building %%C %%P
     msbuild librdkafka.sln /p:Configuration=%%C /p:Platform=%%P /p:PlatformToolset=v140 /target:Clean
     msbuild librdkafka.sln /p:Configuration=%%C /p:Platform=%%P /p:PlatformToolset=v140 || goto :error


  )
)

exit /b 0

:error
echo "Build failed"
exit /b 1
