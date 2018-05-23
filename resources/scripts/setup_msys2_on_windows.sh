
# ONCE YOU HAVE INSTALLED MSYS2 ON WINDOWS...

pacman -S perl
pacman -S tar
pacman -S strip
pacman -S binutils
pacman -S msys/patch
pacman -S msys/make

pacman -S mingw64/mingw-w64-x86_64-lcov
pacman -S mingw64/mingw-w64-x86_64-gcc

pacman -S mingw64/mingw-w64-x86_64-gcc-libs


wget https://aur.archlinux.org/cgit/aur.git/snapshot/lcov.tar.gz
gzip -d lcov.tar.gz
tar -xf lcov.tar
cd lcov

# LIST THE ALTERNATIVES
# pacman -Ss 

makepkg -csi







