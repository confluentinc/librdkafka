# Set up vcpkg and install required packages.

if (!(Test-Path -Path vcpkg/.git)) {
    git clone https://github.com/Microsoft/vcpkg.git
    cd vcpkg
    git checkout 328bd79eb8340b8958f567aaf5f8ffb81056cd36
    cd ..
} else {
    echo "Updating vcpkg git repo"
    cd vcpkg
    git pull
    cd ..
}

.\vcpkg\bootstrap-vcpkg.bat

