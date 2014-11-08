#! /usr/bin/env python

import argparse
import collections
import sqlite3
import urllib2
from datetime import datetime, timedelta

try:
    import boto
    import boto.ec2
except ImportError:
    raise Exception('Please make boto available first.')

all_prices = []


def downloadData(regionsNames, args):
    '''Downloads data from EC2 itself.'''
    from multiprocessing import Pool, cpu_count
    pool = Pool(cpu_count())
    print '\nTotal number of regions: ' + str(len(regionsNames))
    for num, name in enumerate(regionsNames):
        f = pool.apply_async(getSpotPricesFromRegion, [args, num, name], callback=cbLogPrices)
        # This ensures that the error is propagated to stdout should the function throw.
        # Note that this fails if the failure happens if num == 0, 1, 2, 3, ... (except the last)
        # For our purposes here, using multiple threads works though.
        if num == len(regionsNames) - 1:  # Synchronously block on return of only the last iteration
            f.get()
    pool.close()
    pool.join()


def cbLogPrices(pr):
    '''Callback function for apply_async function to append results to a list.'''
    all_prices.append(pr)


def getSpotPricesFromRegion(args, regionNum, regionName):
    '''Gets spot prices of the specified region.'''
    now = datetime.now()
    start = now - timedelta(days=180)  # Use a 6 month range

    print 'Processing region number ' + str(regionNum) + ': ',
    pr = boto.ec2.connect_to_region(regionName,
                                    aws_access_key_id=args.awsKeyId,
                                    aws_secret_access_key=args.awsSecret
                                    ).get_spot_price_history(start_time=start.isoformat(),
                                                             end_time=now.isoformat(),
                                                             instance_type=args.instanceType,
                                                             product_description=args.os)
    print 'Finished getting the prices from: ' + regionName
    return pr


def isInternetOn():
    '''
    Tests if internet connection is working properly.
    Adapted from http://stackoverflow.com/a/3764660.
    '''
    try:
        response = urllib2.urlopen('http://www.google.com/', timeout=1)
        return True
    except urllib2.URLError:
        pass
    return False


def parseArgs():
    '''Parse arguments as specified.'''
    osChoices = ['linux', 'suselinux', 'windows']

    desc = 'Retrieves the latest EC2 prices and displays the lowest 3 and highest priced zones.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-a', '--awsKeyId', required=True, help='Sets the AWS key ID.')
    parser.add_argument('-b', '--awsSecret', required=True, help='Sets the AWS secret access key.')
    parser.add_argument('-t', '--instanceType', default='r3.large',
                        help='Sets the EC2 instance type. Defaults to %(default)s.')
    parser.add_argument('-o', '--os', default='linux', choices=osChoices,
                        help='Sets the operating system. Choose from [' + '|'.join(osChoices) +
                                ']. Defaults to %(default)s.')
    parser.add_argument('-s', '--spawnNum', default=100, type=int,
                        help='Sets the hypothetical number of instances to be spawned.')
    args = parser.parse_args()

    if args.os == 'linux':
        args.os = 'Linux/UNIX'
    elif args.os == 'suselinux':
        args.os = 'SUSE Linux'
    elif args.os == 'windows':
        args.os = 'Windows'

    return args


def analysePrices(args, aP):
    '''Given a set of prices, analyse the results.'''
    with sqlite3.connect(':memory:') as sqdb:
        cur = sqdb.cursor()
        cur.execute("DROP TABLE IF EXISTS EC2Instances")
        cur.execute("CREATE TABLE EC2Instances(Id INTEGER PRIMARY KEY, Region TEXT, Zone TEXT, \
                        InstanceType TEXT, DateTime TEXT, OperatingSystem TEXT, Price FLOAT)")
        for prices in aP:
            for price in prices:
                cur.execute("INSERT INTO EC2Instances(Region, Zone, InstanceType, \
                                DateTime, OperatingSystem, Price) VALUES (?, ?, ?, ?, ?, ?)",
                                (price.region.name, price.availability_zone, price.instance_type,
                                 price.timestamp, price.product_description, price.price))

        sqdb.row_factory = sqlite3.Row
        cur = sqdb.cursor()

        printResults(cur, args)


def printCols(cur):
    '''Prints specified database columns.'''
    for row in cur.fetchall():
        print row
        #print "%s %s %d" % (row["Id"], row["Name"], row["Price"])  # FIXLASTME


def printResults(cur, args):
    # get the lowest 3 unique prices
    # , extract their zones (removing dupes), check these zones' current price,
    # extract only those that are within 10% of the lowest.
    # Then take this as "how many percent" cheaper than the highest price in that period
    # Use question mark notation
    cur.execute('SELECT * FROM EC2Instances WHERE InstanceType=? ORDER BY Price', ('r3.large',))
    printCols(cur)


def main(all_prices):
    args = parseArgs()

    allRegionNames = [str(x).split(':')[1] for x in boto.ec2.regions()]
    # See https://github.com/boto/boto/issues/1951 as to why we reject the following regions.
    regionNames = [x for x in allRegionNames if x not in ['cn-north-1', 'us-gov-west-1']]

    downloadData(regionNames, args)
    analysePrices(args, all_prices)


if __name__ == '__main__':
    main(all_prices)
