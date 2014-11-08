ec2SpotPrices
=============

Uses boto to get spot instance prices and displays zones with the lowest
latest price.

optional arguments:
  -h, --help            show this help message and exit
  -a AWSKEYID, --awsKeyId AWSKEYID
                        Sets the AWS key ID. (Required)
  -b AWSSECRET, --awsSecret AWSSECRET
                        Sets the AWS secret access key. (Required)
  -t INSTANCETYPE, --instanceType INSTANCETYPE
                        Sets the EC2 instance type. Defaults to r3.large.
  -o {linux,suselinux,windows}, --os {linux,suselinux,windows}
                        Sets the operating system. Choose from
                        [linux|suselinux|windows]. Defaults to linux.
  -s SPAWNNUM, --spawnNum SPAWNNUM
                        Sets the hypothetical number of instances to be
                        spawned.
