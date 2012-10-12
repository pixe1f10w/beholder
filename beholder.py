#!/usr/bin/env python
#-*- coding: utf-8 -*-

from config import config
from behold import beholdDaemon
from logging import handlers
import logging
import sys

def die( reason ):
    sys.stderr.write( 'FATAL ERROR: %s\n' % reason )
    sys.exit( 2 )

def parseConfig( filename ):
    c = config()

    try:
	c.read( filename )
    except:
	die( 'can\'t read configuration file in %s' % filename )

    dbopts = c.sectionMap( 'database' )
    rtmpopts = c.sectionMap( 'rtmp' )
    generalopts = c.sectionMap( 'general' )

    # checking config
    if ( ( 'database' not in dbopts ) or
       ( 'username' not in dbopts ) or
       ( 'host' not in dbopts ) or
       ( 'password' not in dbopts ) ):
	   die( 'database configuration is incomplete' )

    if 'host' not in rtmpopts:
	die( 'rtmp host is not defined' )

    if 'log' not in generalopts:
	generalopts[ 'log' ] = './beholder.log'		# setting default value
    if 'loglevel' not in generalopts:
	generalopts[ 'loglevel' ] = 'error'		# setting default value
    if 'pidfile' not in generalopts:
	generalopts[ 'pidfile' ] = '/tmp/beholder.pid'	# setting default value

    return [ dbopts, rtmpopts, generalopts ]

def initLogger( filename, level ):
    logger = logging.getLogger( 'beholder' )
    #handler = logging.FileHandler( filename )
    handler = handlers.TimedRotatingFileHandler( filename, 'midnight', 1 )
    formatter = logging.Formatter( '%(asctime)s - %(levelname)s - %(message)s' )
    handler.setFormatter( formatter )
    logger.addHandler( handler )

    l = { 'info' : logging.INFO,
	  'debug' : logging.DEBUG,
	  'error' : logging.ERROR
	} [ level ]

    logger.setLevel( l )

    return logger

def main():
    #cwd = os.getcwd()
    ( dbopts, rtmpopts, generalopts ) = parseConfig( '/etc/conf.d/beholder' )

    logger = initLogger( generalopts[ 'log' ], generalopts[ 'loglevel' ] )
    #logger.info( 'beholder started. big brother is watching you :)' )

    b = beholdDaemon( generalopts[ 'pidfile' ] )
    #a = application( logger = l, dboptions = dbopts, rtmp = rtmpopts[ 'host' ] )

    if len( sys.argv ) == 2:
	if sys.argv[ 1 ] == 'start':
	    # setting up necessary options
	    b.setLogger( logger )
	    b.setDatabase( dbopts )
	    b.setRTMPHost( rtmpopts[ 'host' ] )

	    if 'rtmpdump' in generalopts:
		b.setBinaryFilename( 'rtmpdump', generalopts[ 'rtmpdump' ] )
	    if 'yamdi' in generalopts:
		b.setBinaryFilename( 'yamdi', generalopts[ 'yamdi' ] )
	    if 'minutes' in generalopts:
		b.setTime( 'interval', int( generalopts[ 'minutes' ] ) )
	    if 'killafter' in generalopts:
		b.setTime( 'killafter', int( generalopts[ 'killafter' ] ) )
	    if 'sidrefresh' in generalopts:
		b.setTime( 'sidrefresh', int( generalopts[ 'sidrefresh' ] ) )
	    if 'overlap' in generalopts:
		b.setTime( 'overlap', int( generalopts[ 'overlap' ] ) )
	    if 'prefix' in generalopts:
		b.setPrefix( generalopts[ 'prefix' ] )

	    print( 'starting beholder...	[success]' )
	    b.start()
#	    print( 'starting beholder...	[succcess]\n' )
	elif sys.argv[ 1 ] == 'stop':
	    b.stop()
	    print( 'stopping beholder...	[success]' )
	elif sys.argv[ 1 ] == 'restart':
#	    b.restart()
	    b.stop()
	    b.start()
	else:
	    die( 'unknown command' )
	sys.exit( 0 )
    else:
	die( "usage: %s start|stop|restart" % sys.argv[ 0 ] )

#    a.run()

if __name__ == '__main__':
    main()

