from apscheduler.scheduler import Scheduler

from dumper import streamDumper
from daemon import Daemon

import logging

import PySQLPool
import datetime

class beholdDaemon( Daemon ):
#class application():
    def __init__( self, pidfile ):
	Daemon.__init__( self, pidfile )
	self.sched = Scheduler()
	self.processingJob = None
	self.sidsJob = None

	self.logger = ''
	self.rtmp = ''

	self.username = ''
	self.password = ''
	self.host = ''
	self.db = ''
	self.connection = ''

	self.prefix = './'

	#self.username = dboptions[ 'username' ]
	#self.password = dboptions[ 'password' ]
	#self.host = dboptions[ 'host' ]
	#self.db = dboptions[ 'database' ]
	
	#try:
	    #self.connection = PySQLPool.getNewConnection( username = self.username, \
							  #password = self.password, \
							  #host = self.host, \
							  #db = self.db )
	#except:
	    #self.logger.error( 'database connection failed' )
	    #die( 'database connection failed' )
	
	self.sids = {}	# sid to cid dictionary
	self.binaries = {}
	self.times = {}
	
	# all default values can be overriden with corresponding methods
	self.binaries[ 'rtmpdump' ] = './rtmpdump'	# default location
	self.binaries[ 'yamdi' ] = './yamdi'		# default location
	self.times[ 'interval' ] = int( 10 )	# default value, FIXME: currently will not work with other 
	self.times[ 'killafter' ] = int( self.times[ 'interval' ]  * 1.5 )	# default value
	self.times[ 'sidrefresh' ] = int( 30 )	# default value
	self.times[ 'overlap' ] = int( 15 )	# default value in seconds

#    def stop( self ):
#	self.logger.info( 'shutting down beholder' )
#	Daemon.stop( self )

    def setDatabase( self, dboptions ):
	self.username = dboptions[ 'username' ]
	self.password = dboptions[ 'password' ]
	self.host = dboptions[ 'host' ]
	self.db = dboptions[ 'database' ]

    def setRTMPHost( self, rtmp ):
	self.rtmp = rtmp

    def setLogger( self, logger ):
	self.logger = logger

    def setBinaryFilename( self, binary, filename ):
	self.binaries[ binary ] = filename

    def setTime( self, name, value ):
	self.times[ name ] = int( value )

    def setPrefix( self, prefix ):
	self.prefix = prefix

    def _dbConnect( self ):
	try:
	    self.connection = PySQLPool.getNewConnection( username = self.username, \
							  password = self.password, \
							  host = self.host, \
							  db = self.db )
	except:
#	    self.logger.error( 'database connection failed' )
	    die( 'database connection failed' )

    #def _signalHandler( signum, frame ):
#	if signum == signal.SIGTERM:
	#self.logger.info( 'beholder shutting down' )
	#sys.exit( 0 )

    def _getSids( self ):
	try:
	    query = PySQLPool.getNewQuery( self.connection )
	    query.Query( 'select sid, url from cam where dump is true' )
	    self.sids = {}
	    for row in query.record:
#	     	print( '%s - %s' % ( row[ 'sid' ], row[ 'url' ] ) )
		self.sids[ row[ 'sid' ] ] = row[ 'url' ]
	    self.logger.info( "sid list updated" )
	except:
	    self.logger.warning( "getSids failed" )

    def _startProcessing( self ):
	now = datetime.datetime.now()
	self.logger.info( 'processing started at %s' % ( now ) )

	#TODO: !!!
	# checking clocks
#	passed = now - self.schedulerStartTime
	if self.lastRun is not None:
	    passed = now - self.lastRun
	    minutes = passed.seconds / 60
	    self.logger.debug( 'minutes passed %s' % minutes )
	    if minutes != self.times[ 'interval' ]:
		m = int( round( now.minute, -1 ) )  # rounding minutes (FIXME: this will work only for 10)

		if m == 60:
		    m, s = 59, 59
		else:
		    s = 0

		self.schedulerStartTime = datetime.datetime( year = now.year, month = now.month, day = now.day, 
							     hour = now.hour, minute = m, second = s )

		if self.schedulerStartTime < now:
		    seld.schedulerStartTime += datetime.timedelta( minutes = self.times[ 'interval' ] )

		# reinit scheduler
		self._unscheduleJobs()
		self._scheduleJobsInit()

		self.lastRun = None

		return		# there are nothing more to do here

	self.logger.debug( 'kill time interval equals %s' % ( self.times[ 'killafter' ] ) )

	suddenDeathTime = self.schedulerStartTime + datetime.timedelta( minutes = self.times[ 'killafter' ] )

	if suddenDeathTime.second == 59:		# case of hh:59:59
	    suddenDeathTime += datetime.timedelta( seconds = 1 )

	self.logger.debug( 'sudden death time is to %s' % ( suddenDeathTime ) )

	self.schedulerStartTime += datetime.timedelta( minutes = self.times[ 'interval' ] )
	
	if self.schedulerStartTime.second == 59:	# case of hh:59:59
	    self.schedulerStartTime += datetime.timedelta( seconds = 1 )
	
	self.logger.info( 'threads will stop recording at %s' % ( self.schedulerStartTime ) )
	self.logger.debug( 'sids are %s' % ( self.sids ) )
	
	for sid in self.sids:
	    self.logger.debug( 'creating thread for %s' % ( sid ) )
	    d = streamDumper( self.logger, self.connection, sid, self.rtmp, self.sids[ sid ],
			      self.schedulerStartTime.strftime( '%d-%m-%Y %H:%M:%S' ), # it's a stop time for thread
			      suddenDeathTime.strftime( '%d-%m-%Y %H:%M:%S' ),
			      self.prefix, self.binaries[ 'rtmpdump' ], self.binaries[ 'yamdi' ] )
	    d.start()
	    self.logger.info( 'thread started for %s sid' % sid )

	self.lastRun = now

    def _initScheduler( self, currentTime ):
	m = int( round( currentTime.minute, -1 ) )  # rounding minutes (FIXME: this will work only for 10) 

	if m == 60:
	    m, s = 59, 59
#	    s = 59
	else:
	    s = 0

	nextTime = datetime.datetime( year = currentTime.year,
				      month = currentTime.month,
				      day = currentTime.day,
				      hour = currentTime.hour,
				      minute = m,
				      second = s )

	if nextTime > currentTime:
	    self.schedulerStartTime = nextTime
	else:
	    self.schedulerStartTime = nextTime + datetime.timedelta( minutes = self.times[ 'interval' ] )
	
	self.logger.info( 'scheduled processing start time is %s' % self.schedulerStartTime )
	
#	self.sched.add_date_job( self._startProcessing, self.schedulerStartTime - datetime.timedelta( seconds = self.times[ 'overlap' ] ) )
#	self.sched.add_date_job( self._setupIntervalScheduler, self.schedulerStartTime - datetime.timedelta( seconds = self.times[ 'overlap' ] ) )
	self._scheduleJobsInit()
	
	self.sched.daemonic = False	# scheduler will not let caller thread exit
	self.sched.start()
	self.logger.info( 'scheduler initialization completed' )

    def _scheduleJobsInit( self ):
	self.lastRun = None
	starttime = self.schedulerStartTime - datetime.timedelta( seconds = self.times[ 'overlap' ] )
#	self.sched.add_date_job( self._startProcessing, self.schedulerStartTime - datetime.timedelta( seconds = self.times[ 'overlap' ] ) )
#	self.sched.add_date_job( self._setupIntervalScheduler, self.schedulerStartTime - datetime.timedelta( seconds = self.times[ 'overlap' ] ) )
	self.sched.add_date_job( self._startProcessing, starttime )
	self.sched.add_date_job( self._setupIntervalScheduler, starttime )

    def _setupIntervalScheduler( self ):
	self.processingJob = self.sched.add_interval_job( self._startProcessing, minutes = int( self.times[ 'interval' ] ) )
	self.sidsJob = self.sched.add_interval_job( self._getSids, minutes = int( self.times[ 'sidrefresh' ] ) )

    def _unscheduleJobs( self ):
	self.sched.unschedule_job( self.processingJob )
	self.sched.unschedule_job( self.sidsJob )

    def run( self ):
#	signal.signal( signal.SIGTERM, self._signalHandler ) # setting up shutdown handler
	self.logger.info( 'beholder started. big brother is watching you :)' )
	self._dbConnect()
	self._getSids()
	self._initScheduler( datetime.datetime.now() )
	#while 1:
	    #pass
