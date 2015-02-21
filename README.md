
sudo sysctl -w kern.maxfilesperproc=200000
sudo sysctl -w kern.maxfiles=200000
sudo sysctl -w net.inet.ip.portrange.first=1024