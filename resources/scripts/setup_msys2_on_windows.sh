
# ONCE YOU HAVE INSTALLED MSYS2 ON WINDOWS...

pacman -S perl
pacman -S tar
pacman -S strip
pacman -S binutils
pacman -S msys/patch
pacman -S msys/make

wget https://aur.archlinux.org/cgit/aur.git/snapshot/lcov.tar.gz
gzip -d lcov.tar.gz
tar -xf lcov.tar
cd lcov

# LIST THE ALTERNATIVES
# pacman -Ss 

makepkg -csi







