# Build guide for Windows

* build.bat - Build for all combos of: Win32,x64,Release,Debug using the current msbuild toolset
* package-zip.ps1 - Build zip package (using build.bat artifacts)
* vcpkg-overlay-ports/ - vcpkg overlay port that keeps the bundled OpenSSL on the 3.5 LTS line (see its README.md)

