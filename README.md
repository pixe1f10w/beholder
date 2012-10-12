# Beholder

Beholder is a RTMP dumping and archiving tool for multi-channel CCTV.

## Depends on

Beholder is written in Python, so Python is obviously needed.

Also some Python libraries are required: [APScheduler](packages.python.org/APScheduler) and [PySQLPool](code.google.com/p/pysqlpool/) which can be installed with `pip`, `easy_install` or with provided `install_dependcies` shell script.

Beholder invokes binaries of [modified rtmpdump](http://github.com/pixe110w/rtmpdump-mod) and [yamdi](http://yamdi.sourceforge.net) for dumping RTMP streams and metadata injection respectively. Paths to these binaries are specified in behold.conf file.

Beholder stores it's data in the MySQL database which credentials are also specified in behold.conf file. Schema for target database provided in `schema.sql` file.

## Operation

Beholder acts as execution daemon for multiple `rtmpdump` and sequential `yamdi` processes. When started, it demonizes and schedules invocation fo these processes for each stream specified in the `cam` table of target database. For each 10 minutes it'll finish dumping to file, initiates the metadata injection and open a new dumping process for each stream. Resulting files are overlapped for 10 (can be changed in behold.conf) seconds. Status of each file is stored to the `rec` table of target database.

## Status

Proved as workable by 1.5+ year of operation in production environment with 8 cameras. Sources are not touched since developed in march 2011.

## License

Copyright (c) 2011,2012 Ilia Zhirov.

Licensed under terms of [MIT license](http://www.opensource.org/licenses/mit-license.php).

Feel free to fork it, fix any bugs, add features, send me a pull requests and so on. Bug reports are also welcome
