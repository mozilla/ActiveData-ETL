
# Scripts

This directory contains scripts that can be used to setup the various ActiveData-ETL machines.  **None if these scripts are meant to be run blindly.** Rather, they serve as a meticulous recording of what was done on the machines they represent. Please understand each step before you run it, and verify its effect after: Of course, if you are familiar with most of the commands, this will go quickly.



## Installing MSYS2 on Windows

The CodeCoverage ETL requires `lcov` to decode the binary `gdsa` `gcno` files. `lcov` is not available on Windows, so we require some Linux emulator to do this for us. MSYS2 can be controlled from the Windows command line; effectively giving the full power of Linux to Windows!! 


Install MSYS2 on your windows machine

	http://repo.msys2.org/distrib/x86_64/msys2-x86_64-20161025.exe

Run it, and install basic packages

	pacman -S perl
	pacman -S tar
	pacman -S strip
	pacman -S binutils
	pacman -S msys/patch
	pacman -S msys/make

Proceed with `lcov` download-and-compile 

	wget https://aur.archlinux.org/cgit/aur.git/snapshot/lcov.tar.gz
	gzip -d lcov.tar.gz
	tar -xf lcov.tar
	cd lcov
	makepkg -csi

Alternatively, you may be able to install directly:

	pacman -S mingw64/mingw-w64-x86_64-lcov 


*Note: Use `pacman -Ss` to list the possible matching packages*

