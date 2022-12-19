# Set up vcpkg and install required packages.

if (!(Test-Path -Path vcpkg/.git)) {
    git clone https://github.com/Microsoft/vcpkg.git
} else {
    echo "Updating vcpkg git repo"
    cd vcpkg
    git pull
    cd ..
}

.\vcpkg\bootstrap-vcpkg.bat

