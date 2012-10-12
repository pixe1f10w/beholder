import ConfigParser

class config( ConfigParser.ConfigParser ):
    #def __init__( self, logger ):
	#ConfigParser.ConfigParser.__init__()
	#self.logger = logger
    def sectionMap( self, section ):
	d = {}
	options = self.options( section )
	for option in options:
	    try:
		d[ option ] = self.get( section, option )
		if d[ option ] == -1:
		    DebugPrint( "skip: %s" % option )
	    except:
		print( "exception on %s!" % option )
		d[ option ] = None
	return d