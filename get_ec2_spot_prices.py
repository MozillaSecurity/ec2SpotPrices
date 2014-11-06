#! /usr/bin/env python

import argparse
import os  # FIXLASTME
import sqlite3
import sys
import urllib2

try:
    import boto
    import boto.ec2
except ImportError:
    raise Exception('Please make boto available first.')

AWS_ACCESS_KEY_ID = 'AKIAIIUEUG23IMVFIBPA'
AWS_SECRET_ACCESS_KEY = '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'

all_prices = []


def downloadData(regionsNames, args):
    '''Downloads data from EC2 itself.'''
    from multiprocessing import Pool, cpu_count
    pool = Pool(cpu_count())
    print 'Total number of regions: ' + str(len(regionsNames))
    for num, name in enumerate(regionsNames):
        f = pool.apply_async(getSpotPricesFromRegion, [name, num], callback=cbLogPrices)
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


def getSpotPricesFromRegion(regionName, regionNum):
    '''Gets spot prices of the specified region.'''
    from datetime import datetime, timedelta
    now = datetime.now()
    start = now - timedelta(0, 180)  # Use a 3 minute range

    print 'Processing region number ' + str(regionNum) + ': ',
    pr = boto.ec2.connect_to_region(regionName,
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                                    ).get_spot_price_history(start_time=start.isoformat(),
                                                             end_time=now.isoformat())
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
    desc = 'Retrieves the latest EC2 prices and displays the lowest 3 and highest priced zones.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-t', '--instanceType', dest='instanceType', default='r3.large',
                        help='Sets the EC2 instance type. Defaults to %(default)s.')
    parser.add_argument('-o', '--opSystem', dest='os', default='linux',
                        help='Sets the operating system. Choose from [linux|windows|all]. ' +
                             'Defaults to %(default)s.')
    parser.add_argument('-s', '--saveFile', dest='saveFile',
                        help='Use local saved file instead of downloading data.')  #FIXLASTME

    args = parser.parse_args()

    args.instanceType = tuple(args.instanceType.split(','))  # Casted to a tuple for valid queries

    if args.os == 'linux':
        args.os = ('Linux/UNIX',)  # Trailing comma forces it to become a tuple, which is needed.
    elif args.os == 'windows':
        args.os = ('Windows',)  # Trailing comma forces it to become a tuple, which is needed.
    elif args.os == 'all':
        args.os = ('Linux/UNIX', 'Windows')
    else:
        raise Exception('Invalid OS, choose from [linux|windows|all]: ' + args.os)

    if args.saveFile:
        args.saveFile = os.path.abspath(os.path.expanduser(args.saveFile))  # FIXLASTME

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

        # These results only make sense when only one type of instance is specified.
        if len(args.instanceType) == 1:
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

    if not (args.saveFile or isInternetOn()):
        raise Exception('Either go online to download data, or specify a local data file.')

    allRegionNames = [str(x).split(':')[1] for x in boto.ec2.regions()]
    # See https://github.com/boto/boto/issues/1951 as to why we reject the following regions.
    regionNames = [x for x in allRegionNames if x not in ['cn-north-1', 'us-gov-west-1']]

    ### FIXLASTME ##################################################################################
    if not args.saveFile or (args.saveFile and not os.path.isfile(args.saveFile)):
        downloadData(regionNames, args)
        if args.saveFile and not os.path.isfile(args.saveFile):
            from pickle import dump as pkdump
            with open(args.saveFile, 'wb') as f:
                pkdump(all_prices, f)
    else:
        from pickle import load as pkload
        with open(args.saveFile, 'rb') as f:
            all_prices = pkload(f)

    analysePrices(args, all_prices)
    ################################################################################################

    #downloadData(regionNames, args)
    #analysePrices(args)  # FIXLASTME: Remove second argument to analysePrices


if __name__ == '__main__':
    try:
        main(all_prices)
    except:  # FIXLASTME
        import traceback; traceback.print_exc()
        import pdb; pdb.post_mortem(sys.exc_info()[2])
