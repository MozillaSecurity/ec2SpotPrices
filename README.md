ec2SpotPrices
=============

Uses boto to get spot instance prices and displays zones with the lowest
latest price.

Daily, weekly, monthly and yearly cost estimates are given, assuming
a number of instances to be spawned. Note that this does not take into
account future price changes or fluctuations.

It requires a valid AWS key ID and secret access key, and it is suggested
that users create a separate key set specially for this purpose.

### Commandline Arguments
```
  -h, --help            show this help message and exit
  -a AWSKEYID, --awsKeyId AWSKEYID
                        Sets the AWS key ID. (Required)
  -b AWSSECRET, --awsSecret AWSSECRET
                        Sets the AWS secret access key. (Required)
  -n SPAWNNUM, --spawnNum SPAWNNUM
                        Sets the hypothetical number of instances to be
                        spawned. Defaults to "100".
  -o {linux,suselinux,windows}, --os {linux,suselinux,windows}
                        Sets the operating system. Choose from
                        [linux|suselinux|windows]. Defaults to "linux".
  -t INSTANCETYPE, --instanceType INSTANCETYPE
                        Sets the EC2 instance type. Defaults to "r3.large".
```
