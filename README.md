ec2SpotPrices
=============

Uses boto to get spot instance prices and displays zones with the lowest
latest price.

Daily, weekly, monthly and yearly cost estimates are given, assuming
a number of instances to be spawned. Note that this does not take into
account future price changes or fluctuations.

It requires a valid AWS key ID and secret access key, and it is suggested
that users create a separate key set specially for this purpose.

A sqlite3 database is generated in-memory to compute the result.

## Strategy

Strategy involves finding lowest ever price across 6 months, then
selecting regions/zones with the lowest latest price.
(and is within 25% of lowest ever price)

The lowest latest price will never be lower than the lowest ever price
across the past 6 months.

## Assumptions
- That 6 months of past data is sufficient
- That 25% is a sufficient barrier to prevent sudden price spikes

### Commandline Arguments
```
  -h, --help            show this help message and exit
  -instance-type INSTANCETYPE
                        Sets the EC2 instance type. Defaults to "r3.large".
  -os {linux,suselinux,windows}
                        Sets the operating system. Choose from
                        [linux|suselinux|windows]. Defaults to "linux".
  -profile PROFILE      AWS profile name in ".boto". Defaults to "laniakea".
  -spawn-num SPAWNNUM   Sets the hypothetical number of instances to be
                        spawned. Defaults to "1".
```
