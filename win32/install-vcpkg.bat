cd %1
IF NOT EXIST vcpkg\bootstrap-vcpkg.bat (
   git clone https://github.com/Microsoft/vcpkg.git
)
.\vcpkg\bootstrap-vcpkg.bat
