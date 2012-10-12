import threading
import subprocess
import re
import logging
import PySQLPool
import datetime
import os

class streamDumper( threading.Thread ):
    def __init__( self, logger, connection, sid, rtmp, playpath, stoptime, killtime, prefix, rtmpdump, yamdi ):
        threading.Thread.__init__( self )
        self.logger = logger
        self.connection = connection
        self.sid = sid
        self.rtmp = rtmp
        self.playpath = playpath
        self.stoptime = stoptime
        self.killtime = killtime
#        self.prefix = './'
	self.prefix = prefix
        self.rtmpdump = rtmpdump
        self.yamdi = yamdi
        now = datetime.datetime.now()
        self.starttime = now.strftime( '%d-%m-%Y %H:%M:%S' )
        self.recid = ''

	self.logger.debug( 'created thread for %s at %s' % ( self.sid, self.starttime ) )

#	now = datetime.datetime.now()
#	starttime = now.strftime( '%d-%m-%Y %H:%M:%S' )
#	( date, time ) = self.stoptime.split()
	( date, time ) = self.starttime.split()
	( day, month, year ) = date.split( '-' )
	year = year[ 2: ]
	( hour, minute, _ ) = time.split( ':' )
	minute = str( int( round( float( minute ), -1 ) ) / 10 )	# FIXME: this will work only for 10 minutes based scheduling

	self.logger.debug( 'minute value equals %s' % minute )

	if minute == '6':		# hour (day, month, year) transition
	    ( date, time ) = self.stoptime.split()
	    ( day, month, year ) = date.split( '-' )
	    year = year[ 2: ]

	    if hour == 23:
		( hour, minute ) = '0', '0'
	    else:
		( hour, _, _ ) = time.split( ':' )
		minute = '0'

	self.filepath = self.prefix.rstrip( '/' ) + '/' \
		      + self.sid + '/' \
		      + year + '/' \
		      + month + '/' \
		      + day + '/' \
		      + hour + '/'
	self.logger.debug( 'dump filepath is \'%s\'' % self.filepath )

	if not os.path.exists( self.filepath ):
	    self.logger.debug( 'dump filepath does not exist, creating \'%s\'' % self.filepath )
	    os.makedirs( self.filepath )

	self.filename = self.filepath + minute
	self.logger.debug( 'dump filename is \'%s\'' % self.filepath )

    #def setStopTime( stoptime ):
	#self.stoptime = stoptime

    def _dump( self ):
	#self.filename = self.prefix +self.stoptime.replace( '-', '' )\
						  #.replace( ':', '' )\
						  #.replace( ' ', '_' )\
						  #+ '_' + self.sid
 
	command = [ self.rtmpdump,  "--quiet", "--live",	# supressing output
		"--rtmp", self.rtmp, 				# rtmp server url
		"--playpath", self.playpath,			# stream name
		"--to", self.stoptime,				# stop datetime
		"--killmeplz", self.killtime,			# sudden death time
		"--flv", self.filename + ".tmp" ]		# output filename

	commandstr = ''
	for arg in command:
	    commandstr += arg + ' '
	commandstr = commandstr.rstrip( ' ' )
	
	self.logger.info( 'command to be executed: \'%s\'' % commandstr )

	p = subprocess.Popen( command )
	p.wait()

	return p.returncode

    def _index( self ):
	command = [ self.yamdi,
		    "-i", self.filename + ".tmp",	# input
		    "-o", self.filename + ".flv",	# output
		    "-x", self.filename + ".xml" ]	# metadata

	commandstr = ''
	for arg in command:
	    commandstr += arg + ' '
	commandstr = commandstr.rstrip( ' ' )
	
	self.logger.info( 'command to be executed: \'%s\'' % commandstr )

	try:
	    p = subprocess.Popen( command )
	    p.wait()
	    return p.returncode
	except:
	    self.logger.error( 'yamdi failed due absense of file' )
	    return -1

    def _processMeta( self ):
	regex = re.compile( '<(.*?)>(.*?)</(.*?)>' )
	try:
	    f = open( self.filename + '.xml', 'r' )
	    self.keys = {}
	    for l in f:
		items = regex.findall( l )
		for item in items:
		    key = items[ 0 ][ 0 ]
		    value= items[ 0 ][ 1 ]
		    if 'value' not in key:
			self.keys[ key ] = value
	    return 0	# successful processing
	except:
	    return -1

    def _cleanup( self ):
	try:
	    command = [ "rm", "-f", self.filename + ".tmp", self.filename + ".xml" ]

	    commandstr = ''
	    for arg in command:
		commandstr += arg + ' '
	    commandstr = commandstr.rstrip( ' ' )
	
	    self.logger.info( 'command to be executed: \'%s\'' % commandstr )

	    p = subprocess.Popen( command )
	    p.wait()	
	except:
	    self.logger.warning( "cleanup failed due absense of file" )

    def _dbQuery( self, query, querystring, querytype = 'update' ):
	try:
#	    query = PySQLPool.getNewQuery( self.connection )
	    self.logger.debug( 'prepared query \'%s\'' % querystring )
	    query.Query( querystring )
	    self.logger.debug( 'database query completed' )
	    if querytype == 'insert':
		return query.lastInsertID
	    else:
		return 1
	except:
	    self.logger.debug( 'database query error' )
	    return -1

    def run( self ):
	try:
	    poolQuery = PySQLPool.getNewQuery( self.connection, commitOnEnd = True )
	except:
	    self.logger.error( 'can\'t retrieve new pool query for thread' )
	    return
	#self.logger.info( 'initiating
	#query = 'insert into rec( sid_cam, sid_status, dt ) values( \'%s\', \'created\', \'%s\' )' % ( str( self.sid ), self.starttime )
	query = 'insert into rec( sid_cam, sid_status ) values( \'%s\', \'created\' )' % str( self.sid )
	
	self.recid = self._dbQuery( poolQuery, query, 'insert' )
	if self.recid == -1:
	    self.logger.error( 'database insert error for sid \'%s\'' % self.sid )
	    return

	self.logger.info( 'initiating dumping' )
	r = self._dump()
	self.logger.debug( 'dumping completed with return code %s' % r )

	if r != 0:
	    self.logger.warning( 'rtmdump failed' )	# don't kill thread in that case
#	    self.logger.error( 'rtmpdump failed' )
#	    return

	query = 'update rec set sid_status = \'dumped\' where id = \'%s\'' % self.recid
	if self._dbQuery( poolQuery, query ) == -1:
	    self.logger.error( 'database update error for sid \'%s\'' % self.sid )
	    return

	self.logger.info( 'initiating indexing' )
	r = self._index()
	self.logger.debug( 'indexing completed with return code %s' % r )

	if r != 0:
	    self.logger.error( 'yamdi failed' )
	    return
	
	query = 'update rec set sid_status = \'tagged\' where id = \'%s\'' % self.recid
	if self._dbQuery( poolQuery, query ) == -1:
	    self.logger.error( 'database update error for sid \'%s\'' % self.sid )
	    return

	self.logger.info( 'initiating metadata analysis' )
	r = self._processMeta()
	self.logger.debug( 'metadata analysis completed with return code %s' % r )

	if ( r != 0 ) or ( 'duration' not in self.keys ):
	    self.logger.error( 'metadata analysis failed' )
	    return

	query = 'update rec set sid_status = \'ready\', duration = \'%s\' where id = \'%s\'' \
		% ( int( float( self.keys[ 'duration' ] ) ), self.recid )
	if self._dbQuery( poolQuery, query ) == -1:
	    self.logger.error( 'database update error for sid \'%s\'' % self.sid )
	    return

	self.logger.info( 'initiating cleanup' )
	r = self._cleanup()
	self.logger.debug( 'cleanup completed with return code %s' % r )

	self.logger.info( 'all operations completed. thread should be terminated now' )

