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

    pr = boto.ec2.connect_to_region(regionName,
                                    profile_name = args.profile
                                    ).get_spot_price_history(start_time=start.isoformat(),
                                                             end_time=now.isoformat(),
                                                             instance_type=args.instanceType,
                                                             product_description=args.os)
    print 'Finished getting the prices from: ' + regionName
    return pr


def isInternetOn():
    '''Tests if internet connection is working, adapted from http://stackoverflow.com/a/3764660'''
    try:
        response = urllib2.urlopen('http://www.amazon.com/', timeout=1)
        return True
    except urllib2.URLError:
        pass
    return False


def parseArgs():
    '''Parse arguments as specified.'''
    osChoices = ['linux', 'suselinux', 'windows']

    desc = 'Uses boto to get spot instance prices and displays zones with the lowest latest price.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-n', '--spawnNum', default=100, type=int,
                        help='Sets the hypothetical number of instances to be spawned. ' +
                                'Defaults to "%(default)s".')
    parser.add_argument('-o', '--os', default='linux', choices=osChoices,
                        help='Sets the operating system. Choose from [' + '|'.join(osChoices) +
                                ']. Defaults to "%(default)s".')
    parser.add_argument('-p', '--profile', default='laniakea', help='AWS profile name in .boto')
    parser.add_argument('-t', '--instanceType', default='r3.large',
                        help='Sets the EC2 instance type. Defaults to "%(default)s".')
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


def printResults(cur, args):
    '''Print region(s) and zone(s) of lowest latest price that is within 25% of the lowest ever.'''
    # Fetch regions/zones at where the lowest prices occurred.
    regionsZones = collections.OrderedDict()
    cur.execute('SELECT Id, Region, Zone, Price FROM EC2Instances \
                    WHERE InstanceType=? AND OperatingSystem=? ORDER BY Price ASC',
                    (args.instanceType, args.os))
    for row in cur.fetchall():
        key = 'dbId-{0}_price-{1}'.format(row['Id'], row['Price'])
        regionsZones[key] = row['Region'] + '_' + row['Zone']

    lowestLatestPrice = -1
    lowestRegionsZones = {}
    zonesConsidered = []
    print '\nList of prices:\n'
    # Check the latest prices of these zones, because the lowest may not be the latest.
    for idPrice, values in regionsZones.iteritems():
        dbId, lowestPrice = idPrice[5:].split('_price-')
        region, zone = regionsZones[idPrice].split('_')

        if zone not in zonesConsidered:
            cur.execute('SELECT Price FROM EC2Instances \
                            WHERE InstanceType=? AND OperatingSystem=? AND Region=? AND Zone=? \
                            ORDER BY DateTime DESC LIMIT 1',
                            (args.instanceType, args.os, region, zone))
            latestPrice = cur.fetchone()[0]
            percentDiff = (latestPrice - float(lowestPrice)) * 10000 / (float(lowestPrice) * 100)
            assert percentDiff >= 0, 'The latest price "{0}" cannot be '.format(latestPrice) + \
                                        'lower than the lowest price "{0}".'.format(lowestPrice)
            zonesConsidered.append(zone)

            print 'Region with a lowest price of: US$ {0:.4f} and '.format(float(lowestPrice)) + \
                    'a latest price of: US$ {0:.4f} - (difference of {1:.3g}%):\t{2}\tZone: {3}'\
                    .format(float(latestPrice), percentDiff, region, zone)

            # If latest price differs from lowest by ~25% or more, ignore due to price volatility
            if percentDiff < 25:
                if lowestLatestPrice == -1 or latestPrice < lowestLatestPrice:
                    lowestLatestPrice = latestPrice
                    lowestRegionsZones[regionsZones[idPrice]] = latestPrice

                # It is okay to have multiple regions having the lowest latest price.
                if lowestLatestPrice == latestPrice and \
                        regionsZones[idPrice] not in lowestRegionsZones.keys():
                    lowestRegionsZones[regionsZones[idPrice]] = latestPrice

    if lowestLatestPrice == -1:
        raise Exception('No price history obtained.')

    # Remove regions/zones that do not have the lowest latest price.
    lowestRegionsZones = {k: v for k, v in lowestRegionsZones.iteritems() if v == lowestLatestPrice}

    print '\n======================================================================================'
    print '\nSummary of lowest latest price{0}, available at the following zone{0}:\n'\
            .format('s' if len(lowestRegionsZones.keys()) > 1 else '')

    for regionZone, lowestPrice in lowestRegionsZones.iteritems():
        region, zone = regionZone.split('_')
        print '\tRegion:\t\t' + region
        print '\tZone:\t\t' + zone
        print '\t\t======'

    print '\n\tInstance:\t{0}'.format(args.instanceType)
    print '\tOS:\t\t{0}\n'.format(args.os)
    print '\tUnit price:\tUS$ {0}\n'.format(lowestLatestPrice)

    # Calculate daily, weekly, monthly and yearly costs of running the specified spot instance type.
    freqDict = collections.OrderedDict({'hourly': 1})
    freqDict['daily'] = freqDict['hourly'] * 24
    freqDict['weekly'] = freqDict['daily'] * 7
    freqDict['monthly'] = freqDict['daily'] * 365 / 12
    freqDict['yearly'] = freqDict['daily'] * 365
    print '\tQuantity:\t{0:,}'.format(args.spawnNum)
    for freq, mul in freqDict.iteritems():
        print '\t{0} price:\tUS$ {1:,.2f}'\
            .format(freq.capitalize(), mul * lowestLatestPrice * args.spawnNum)
    print


def main(all_prices):
    args = parseArgs()

    allRegionNames = [str(x).split(':')[1] for x in boto.ec2.regions()]
    # See https://github.com/boto/boto/issues/1951 as to why we reject the following regions.
    regionNames = [x for x in allRegionNames if x not in ['cn-north-1', 'us-gov-west-1']]

    downloadData(regionNames, args)
    analysePrices(args, all_prices)


if __name__ == '__main__':
    main(all_prices)
